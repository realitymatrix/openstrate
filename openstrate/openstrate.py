import os
import sys
import time
import queue
import getopt
import logging
import requests
import websocket
from json import dumps
from uuid import uuid4
from nanoid import generate
from threading import Thread
from dotenv import load_dotenv
from os.path import basename, dirname
from tornado.escape import json_decode, json_encode, utf8

load_dotenv()
API_PROTOCOL = 'http'
API_HOST = os.getenv('API_HOST')
API_PORT = '3333'
PYTHON_INTERPRETER = 'python3'
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
API_URL = f'{API_PROTOCOL}://{API_HOST}:{API_PORT}'
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 120))
HTTP_API_ENDPOINT = f'http://{API_HOST}:8888/api/kernels'
WS_API_ENDPOINT = f'ws://{API_HOST}:8888/api/kernels'


#   Types of requests available to openstrate
class RequestType:

    GET = 'get'
    POST = 'post'
    PUT = 'put'
    PATCH = 'patch'
    DELETE = 'delete'

#   Types of HTTP response status codes
class ResponseCode:
    OK          = 200
    CREATED     = 201
    NO_CONTENT  = 204


class KernelInitializationException(Exception):
    def __init__(self):
        print('[KernelInitializationException]:     Failed to initialize the kernel.')

class WebsocketIntitializationException(Exception):
    def __init__(self):
        print('[WebsocketIntitializationException]: Failed to initialize the websocket connection.')

class ScriptExecutionFailureException(Exception):
    def __init__(self):
        print('[ScriptExecutionFailureException]:   Failed to execute a script on the remote machine.')

class KernelTerminationException(Exception):
    def __init__(self):
        print('[KernelTerminationException]:        Failed to terminate the kernel.')

class FileSystemException(Exception):
    def __init__(self):
        print('[FileSystemException]:               The filesystem operation could not be completed.')

class UnexpectedResponseException(Exception):
    def __init__(self):
        print('[UnexpectedResponseException]:       The server responded with an unexpected response code.')


class KernelClient():
    
    DEAD_MSG_ID = "deadmsgid"
    POST_IDLE_TIMEOUT = 0.5
    DEFAULT_INTERRUPT_WAIT = 1

    def __init__(self, http_api_endpoint, ws_api_endpoint, kernel_id, timeout=REQUEST_TIMEOUT, logger=None):
        self.response_reader = Thread(target=self._read_responses)
        self.http_api_endpoint = http_api_endpoint
        self.kernel_http_api_endpoint = f"{http_api_endpoint}/{kernel_id}"
        self.ws_api_endpoint = ws_api_endpoint
        self.kernel_ws_api_endpoint = f"{ws_api_endpoint}/{kernel_id}/channels"
        self.shutting_down = False
        self.restarting = False
        self.kernel_id = kernel_id
        self.kernel_socket = None
        self.log = logger
        self.log.debug(f"Initializing kernel client ({kernel_id}) to {self.kernel_ws_api_endpoint}")
        self.response_queues = {}
        self.access_token = ACCESS_TOKEN
        try:
            self.kernel_socket = websocket.create_connection(
                f"{ws_api_endpoint}/{kernel_id}/channels?token={self.access_token}", timeout=timeout, enable_multithread=True
            )
        except Exception as e:
            self.log.error(e)
            self.shutdown()
            raise e
        # startup reader thread
        self.response_reader.start()
    
    def shutdown(self):
        """Shut down the client."""
        # Terminate thread, close socket and clear queues.
        self.shutting_down = True

        if self.kernel_socket:
            self.kernel_socket.close()
            self.kernel_socket = None

        if self.response_queues:
            self.response_queues.clear()
            self.response_queues = None

        if self.response_reader:
            self.response_reader.join(timeout=2.0)
            if self.response_reader.is_alive():
                self.log.warning("Response reader thread is not terminated, continuing...")
            self.response_reader = None

        url = f"{self.http_api_endpoint}/{self.kernel_id}?token={self.access_token}"
        response = requests.delete(url, timeout=60)
        if response.status_code == ResponseCode.NO_CONTENT:
            self.log.info(f"Kernel {self.kernel_id} shutdown")
            return True
        else:
            msg = f"Error shutting down kernel {self.kernel_id}: {response.content}"
            raise RuntimeError(msg)
    
    def execute(self, code, timeout=REQUEST_TIMEOUT):
        """
        Executes the code provided and returns the result of that execution.
        """
        response = []
        has_error = False
        try:
            msg_id = self._send_request(code)

            post_idle = False
            while True:
                response_message = self._get_response(msg_id, timeout, post_idle)
                if response_message:
                    response_message_type = response_message["msg_type"]

                    if response_message_type == "error" or (
                        response_message_type == "execute_reply"
                        and response_message["content"]["status"] == "error"
                    ):
                        has_error = True
                        response.append(
                            "{}:{}:{}".format(
                                response_message["content"]["ename"],
                                response_message["content"]["evalue"],
                                response_message["content"]["traceback"],
                            )
                        )
                    elif response_message_type == "stream":
                        response.append(
                            KernelClient._convert_raw_response(response_message["content"]["text"])
                        )

                    elif (
                        response_message_type == "execute_result"
                        or response_message_type == "display_data"
                    ):
                        if "text/plain" in response_message["content"]["data"]:
                            response.append(
                                KernelClient._convert_raw_response(
                                    response_message["content"]["data"]["text/plain"]
                                )
                            )
                        elif "text/html" in response_message["content"]["data"]:
                            response.append(
                                KernelClient._convert_raw_response(
                                    response_message["content"]["data"]["text/html"]
                                )
                            )
                    elif response_message_type == "status":
                        if response_message["content"]["execution_state"] == "idle":
                            post_idle = True  # indicate we're at the logical end and timeout poll for next message
                            continue
                    else:
                        self.log.debug(
                            "Unhandled response for msg_id: {} of msg_type: {}".format(
                                msg_id, response_message_type
                            )
                        )

                if (
                    response_message is None
                ):  # We timed out.  If post idle, its ok, else make mention of it
                    if not post_idle:
                        self.log.warning(
                            "Unexpected timeout occurred for msg_id: {} - no 'idle' status received!".format(
                                msg_id
                            )
                        )
                    break

        except Exception as e:
            self.log.debug(e)

        return "".join(response), has_error
    
    def _send_request(self, code):
        """
        Builds the request and submits it to the kernel.  Prior to sending the request it
        creates an empty response queue and adds it to the dictionary using msg_id as the
        key.  The msg_id is returned in order to read responses.
        """
        msg_id = uuid4().hex
        message = KernelClient.__create_execute_request(msg_id, code)

        # create response-queue and add to map for this msg_id
        self.response_queues[msg_id] = queue.Queue()

        self.kernel_socket.send(message)

        return msg_id

    def _get_response(self, msg_id, timeout, post_idle):
        """
        Pulls the next response message from the queue corresponding to msg_id.  If post_idle is true,
        the timeout parameter is set to a very short value since a majority of time, there won't be a
        message in the queue.  However, in cases where a race condition occurs between the idle status
        and the execute_result payload - where the two are out of order, then this will pickup the result.
        """

        if post_idle and timeout > KernelClient.POST_IDLE_TIMEOUT:
            timeout = (
                KernelClient.POST_IDLE_TIMEOUT
            )  # overwrite timeout to small value following idle messages.

        msg_queue = self.response_queues.get(msg_id)
        try:
            self.log.debug(f"Getting response for msg_id: {msg_id} with timeout: {timeout}")
            response = msg_queue.get(timeout=timeout)
            self.log.debug(
                "Got response for msg_id: {}, msg_type: {}".format(
                    msg_id, response["msg_type"] if response else "null"
                )
            )
        except queue.Empty:
            response = None

        return response

    def _read_responses(self):
        """
        Reads responses from the websocket.  For each response read, it is added to the response queue based
        on the messages parent_header.msg_id.  It does this for the duration of the class's lifetime until its
        shutdown method is called, at which time the socket is closed (unblocking the reader) and the thread
        terminates.  If shutdown happens to occur while processing a response (unlikely), termination takes
        place via the loop control boolean.
        """
        try:
            while not self.shutting_down:
                try:
                    raw_message = self.kernel_socket.recv()
                    response_message = json_decode(utf8(raw_message))

                    msg_id = KernelClient._get_msg_id(response_message, self.log)

                    if msg_id not in self.response_queues:
                        # this will happen when the msg_id is generated by the server
                        self.response_queues[msg_id] = queue.Queue()

                    # insert into queue
                    self.log.debug(
                        "Inserting response for msg_id: {}, msg_type: {}".format(
                            msg_id, response_message["msg_type"]
                        )
                    )
                    self.response_queues.get(msg_id).put_nowait(response_message)
                except BaseException as be1:
                    if (
                        self.restarting
                    ):  # If restarting, wait until restart has completed - which includes new socket
                        i = 1
                        while self.restarting:
                            if i >= 10 and i % 2 == 0:
                                self.log.debug(f"Still restarting after {i} secs...")
                            time.sleep(1)
                            i += 1
                        continue
                    raise be1

        except websocket.WebSocketConnectionClosedException:
            pass  # websocket closure most likely due to shutdown

        except BaseException as be2:
            if not self.shutting_down:
                self.log.warning(f"Unexpected exception encountered ({be2})")

        self.log.debug("Response reader thread exiting...")

    @staticmethod
    def _get_msg_id(message, logger):
        msg_id = KernelClient.DEAD_MSG_ID
        if message:
            if "msg_id" in message["parent_header"] and message["parent_header"]["msg_id"]:
                msg_id = message["parent_header"]["msg_id"]
            elif "msg_id" in message:
                # msg_id may not be in the parent_header, see if present in response
                # IPython kernel appears to do this after restarts with a 'starting' status
                msg_id = message["msg_id"]
        else:  # Dump the "dead" message...
            logger.debug(f"+++++ Dumping dead message: {message}")
        return msg_id

    @staticmethod
    def _convert_raw_response(raw_response_message):
        result = raw_response_message
        if isinstance(raw_response_message, str) and "u'" in raw_response_message:
            result = raw_response_message.replace("u'", "")[:-1]

        return result

    @staticmethod
    def __create_execute_request(msg_id, code):
        return json_encode(
            {
                "header": {
                    "username": "",
                    "version": "5.0",
                    "session": "",
                    "msg_id": msg_id,
                    "msg_type": "execute_request",
                },
                "parent_header": {},
                "channel": "shell",
                "content": {
                    "code": "".join(code),
                    "silent": False,
                    "store_history": False,
                    "user_expressions": {},
                    "allow_stdin": False,
                },
                "metadata": {},
                "buffers": {},
            }
        )



class User:
    '''
        Class responsible for creating, deleting, editing and authentication
    '''

    def _serializeUser(self):
        ''' Serialize the user object for easier parsing '''
        return {
            'email': self.email,
            'id': self.id,
            'access_T': self.access_T,
            'refresh_T': self.refresh_T,
        }
  

    def _createUser(self, email: str, passwd: str):
        ''' Create the user on the server via REST API '''
        headers = {
            "Content-Type": "application/json"
        }
        body = {
                'email': email,
                'password': passwd
        }
        try:
            reg_res = requests.post(
                headers=headers,
                url=f'{API_URL}/users',
                data=dumps(body)
            )
            assert reg_res.status_code == ResponseCode.CREATED, UnexpectedResponseException
            self.id = reg_res.json()["id"]
            tokens = self._authenticateUser(email, passwd)
            self.access_T = tokens['access']
            self.refresh_T = tokens['refresh']
            usr = self._serializeUser()
            return usr
        except UnexpectedResponseException:
            pass
    

    def _authenticateUser(self, email: str, passwd: str):
        ''' Retrieve access and refresh tokens from the server with valid user data '''
        headers = {
            "Content-Type": "application/json"
        }
        body = {
                'email': email,
                'password': passwd
        }
        try:
            auth_res = requests.post(
                headers=headers,
                url=f'{API_URL}/auth',
                data=dumps(body)
            )
            assert auth_res.status_code == ResponseCode.CREATED, UnexpectedResponseException
            return {'access': auth_res.json()["accessToken"], 
                    'refresh': auth_res.json()["refreshToken"]}
        except UnexpectedResponseException:
            pass
    

    def _deleteUser(self):
        ''' Deletes the specified user and removes all data '''
        headers = {
            "Content-Type": "application/json",
            "Authorization": f'Bearer {self.access_T}'
        }
        try:
            del_res = requests.delete(
                headers=headers,
                url=f'{API_URL}/users/{self.id}'
            )
            assert del_res.status_code == ResponseCode.NO_CONTENT, UnexpectedResponseException
        except UnexpectedResponseException:
            pass
    

    def __init__(self, 
                email=None, 
                passwd=None,
                id=None,  
                accessToken=None,
                refreshToken=None):
        ''' Initialize user class, generate id and password if none are provided (embedded implementation) '''
        self.email = email
        self.id = id
        self.passwd = passwd
        self.access_T = accessToken
        self.refresh_T = refreshToken
        self.commands = []
        if not any([email, id, passwd]):
            self.email = f'{generate()}@openstrate.net'
            self.passwd = str(uuid4()).replace('-', '')
        self.new_usr = self._createUser(email=self.email, passwd=self.passwd)
    
    def delete(self):
        self._deleteUser()
    
    def __del__(self):
        self._deleteUser()



class CommandManager:
    '''
        Handles REST commands
    '''

    def __init__(self, user: User):
        ''' Authenticated user is required to run commands '''
        self.user = user

    def createCommand(self, command: str, type: str):
        ''' Create command via REST interface '''
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.user.access_T}"
        }
        body = {
                'command': command,
                'userId': self.user.id,
                'type': type
        }
        try:
            command_res = requests.post(
                headers=headers,
                url=f'{API_URL}/commands/commandString',
                data=dumps(body))
            assert command_res.status_code == ResponseCode.CREATED, UnexpectedResponseException
            return command_res.json()["_id"]
        except UnexpectedResponseException:
            pass

    
    def deleteCommand(self, command_id: str):
        ''' Delete command via REST interface '''
        headers = {
            "Content-Type": "application/json",
            "Authorization": f'Bearer {self.user.access_T}'
        }
        body = {
            'commandId': command_id,
            'userId': self.user.id,
        }
        try:
            del_res = requests.delete(
                headers=headers,
                data=dumps(body),
                url=f'{API_URL}/commands/commandId'
            )
            assert del_res.status_code == ResponseCode.OK, UnexpectedResponseException
        except UnexpectedResponseException:
            pass


    def runCommand(self, command_id: str, data: str = None):
        ''' Run command via REST interface '''
        headers = {
            "Content-Type": "application/json",
            "Authorization": f'Bearer {self.user.access_T}'
        }
        if data:
            body = {
                'commandId': command_id,
                'userId': self.user.id,
                'data': data
            }
        else:
            body = {
                'commandId': command_id,
                'userId': self.user.id
            }
        try:
            run_res = requests.post(
                headers=headers,
                data=dumps(body),
                url=f'{API_URL}/commands/runCommand')
            assert run_res.ok, UnexpectedResponseException
            response = run_res.json()
            return response['result']['data']
        except UnexpectedResponseException:
            pass
    
    def createRunCommand(self, 
                         command: str, 
                         type: str, 
                         data = None):
        ''' Helper function that combines creating and running a command '''
        if data:
            try:
                return self.runCommand(self.createCommand(command, type), data)
            except UnexpectedResponseException:
                pass
        else:
            try:
                return self.runCommand(self.createCommand(command, type), data=None)
            except UnexpectedResponseException:
                pass


class Kernel:
    ''' 
        Kernel objects which can be connected to via websocket connection
    '''

    def _createKernel(self, user: User):
        ''' 
            Creates a new Python kernel, inherits the User object
                @param: user (User) 
        '''
        data = {
            'name': PYTHON_INTERPRETER
        }
        try:
            post_to_kernels = CommandManager(
                user).createRunCommand(
                    command=self._route, 
                    type=RequestType.POST,
                    data=json_encode(data))
            kernel_id = post_to_kernels['id']
            return kernel_id      
        except KernelInitializationException:
            pass
    

    def _createKernelCWD(self, user: User, cwd: str):
        ''' 
            Creates a new kernel at specific working directory
                @param: user (User) 
                @param: cwd  (str) 
        '''
        data = {
            'name': PYTHON_INTERPRETER,
            'path': cwd
        }
        try:
            post_to_kernels = CommandManager(
                user).createRunCommand(
                    command=self._route, 
                    type=RequestType.POST,
                    data=json_encode(data))
            kernel_id = post_to_kernels['id']
            return kernel_id      
        except KernelInitializationException:
            pass


    def _createWebsocket(self, kernel_id: str):
        ''' 
            Creates a websocket connection to specific kernel
                @param: kernel_id  (str) 
        '''
        log = logging.getLogger('KernelClient')
        try:
            return KernelClient(
                HTTP_API_ENDPOINT,
                WS_API_ENDPOINT,
                kernel_id,
                timeout=60,
                logger=log)
        except WebsocketIntitializationException:
            pass


    def _deleteKernel(self, user: User, kernel_id: str):
        delete_to_kernels = CommandManager(user).createCommand(f'api/kernels/{kernel_id}', 'delete')
        headers = {
            "Content-Type": "application/json",
            "Authorization": f'Bearer {user.access_T}'
        }
        body = {
            'commandId': delete_to_kernels,
            'userId': user.id
        }
        try:
            run_res = requests.post(
                headers=headers,
                data=dumps(body),
                url=f'{API_URL}/commands/runCommand'
            )
            assert run_res.status_code == ResponseCode.NO_CONTENT, KernelTerminationException
        except KernelTerminationException:
            pass
        

    def __init__(self, user = None):
        ''' Initialization of the Kernel class starts with authentication pipeline '''
        self._route = 'api/kernels'
        if user == None:
            self._user = User()
        else:
            self._user = user
        try:
            self._kernel = self._createKernel(self._user)
            self.comm = self._createWebsocket(self._kernel)
        except KernelInitializationException:
            pass



# Initialization of openstrate command line console
def write():
    kernel = Kernel().comm
    print("Enter/Paste your content. Ctrl-D to save it. (Ctrl-Z + Enter on Windows)")
    while True:
        contents = []
        while True:
            try:
                line = input('[openstrate] >>> ')
            except EOFError:
                break
            contents.append('\n')
            contents.append(line)
        print(kernel.execute(contents)[0].strip())


# Writes the entire contents of a python file into console and runs
def run_local_file(filepath: str):
    kernel = Kernel().comm
    contents = []
    with open(filepath, mode='r', encoding='utf8') as file:
        for line in file:
            contents.append(line)
        file.close()
    result = kernel.execute(contents)[0].strip()
    kernel.shutdown()
    return result    


class FileSystem:
    ''' Functions for the Cloud file system '''

    def __init__(self, user: User):
        self._route = 'api/contents'
        self._user = user
    

    def check_file_exists(self, filepath: str):
        ''' 
            Checks if a file exists on the cloud on the 
                @param: filepath (str) 
        '''
        command = f'{self._route}{filepath}'
        data = {
            'type': 'file',
            'path': f'{filepath}'
        }
        try:
            get_filesystem = CommandManager(
                self._user).createRunCommand(
                    command=command,
                    type=RequestType.GET,
                    data=json_encode(data))
        except FileSystemException:
            pass
        for key in get_filesystem:
            if key == 'message':
                print(get_filesystem[key])
                return False
            elif key == 'name':
                return (get_filesystem[key] == basename(filepath))
    

    def _run_remote_file(self, filepath: str):
        ''' 
            Run a remote file on the remote 
                @param: filepath (str) 
        '''
        try:
            kernel = Kernel(self._user).comm
            command = f'!{PYTHON_INTERPRETER} {filepath}'
            result = kernel.execute(command)[0].strip()
            kernel.shutdown()
            print(result)
            return result
        except ScriptExecutionFailureException:
            pass
        

    def run_script(self, path_to_script: str):
        ''' 
            Runs a script on the cloud, checks that script exists
                @param: filepath (str) 
        '''
        # assert self.check_file_exists(filepath=path_to_script), FileSystemException
        return self._run_remote_file(filepath=path_to_script)
    


    def file_contents(self, filepath: str):
        ''' 
            Fetches the contents of a file on the cloud 
                @param: filepath (str) 
        '''
        command = f'{self._route}{filepath}'
        data = {
            'type': 'file',
            'path': f'{filepath}'
        }
        try:
            get_filesystem = CommandManager(
                self._user).createRunCommand(
                    command=command,
                    type=RequestType.GET,
                    data=json_encode(data))
            return get_filesystem
        except FileSystemException:
            return None


    def directory_contents(self, directory: str):
        ''' 
            Fetches the contents of a directory on the cloud 
                @param: directory (str)
        '''
        command = f'{self._route}{directory}'
        data = {
            'type': 'directory',
            'path': f'{directory}'
        }
        try:
            get_filesystem = CommandManager(
                self._user).createRunCommand(
                    command=command,
                    type=RequestType.GET,
                    data=json_encode(data))
            assert get_filesystem, FileSystemException
            return get_filesystem
        except FileSystemException:
            return None
    

    def create_directory(self, directory: str):
        """ 
            Responsible for creating directories on the remote
                 @param: directory (str)
        """
        command = f'{self._route}{directory}'
        data = {
            'format': 'json',
            'type': 'directory',
            'path': f'{directory}',
            'name': f'{directory}'
        }
        try:
            post_to_filesystem = CommandManager(
                self._user).createRunCommand(
            command=command,
            type=RequestType.PUT,
            data=json_encode(data))
            return post_to_filesystem
        except FileSystemException:
            pass


    def create_file(self, filepath: str):
        '''
            This function will create a file on the cloud with the same
            extension and filename.

                @param: filepath (str) - path to the file on the cloud
        '''
        filename = basename(filepath)
        file = filepath.split('.')[0]
        extension = filename.split('.')[1]
        command = f'{self._route}{file}.{extension}'
        data = {
            'name': file,
            'type': 'file',
            'format': 'text',
            'path': filepath,
        }
        try:
            type = RequestType.PUT
            post_to_filesystem = CommandManager(self._user).createRunCommand(
                command=command,
                type=type,
                data=json_encode(data))
            return post_to_filesystem
        except FileSystemException:
            pass
    

    def copy_local_file(self, sourcefile_path: str, filepath: str):
        '''
            This function will determine if a file exists on the local machine,
            and if it detects a file on the same path, it will load the contents
            into memory and create a copy of this file on the cloud.

            @param: sourcefile_path (str) - path to the local file
            @param: filepath        (str) - path to the file on the cloud
        '''
        filename = basename(filepath)
        file = filepath.split('.')[0]
        extension = filename.split('.')[1]
        command = f'{self._route}{file}.{extension}'
        file_data = None
        if os.path.exists(sourcefile_path):
            with open(file=sourcefile_path, mode='r') as content:
                file_data = content.read()
            content.close()
        data = {
            'name': file,
            'type': 'file',
            'format': 'text',
            'path': filepath,
            'content': file_data
        }
        try:
            type = RequestType.PUT
            post_to_filesystem = CommandManager(self._user).createRunCommand(
                command=command,
                type=type,
                data=json_encode(data))
            # print(post_to_filesystem)
            return post_to_filesystem
        except FileSystemException:
            pass


    # Deletes a file on the cloud
    def delete_file(self, filepath: str):
        '''
            This function will delete the file on the cloud on the path provided.

                @param: filepath (str) - path to the file on the cloud
        '''
        filename = basename(filepath)
        file = filepath.split('.')[0]
        extension = filename.split('.')[1]
        command = f'{self._route}{file}.{extension}'
        data = {
            'path': filepath
        }
        try:
            type = RequestType.DELETE
            delete_to_filesystem = CommandManager(self._user).createRunCommand(
                command=command,
                type=type,
                data=json_encode(data))
            return delete_to_filesystem
        except FileSystemException:
            pass
    

    # Deletes a directory on the cloud
    def delete_directory(self, directory: str):
        '''
            This function will delete the file on the cloud on the path provided.

                @param: filepath (str) - path to the file on the cloud
        '''
        base_directory = directory.split('/')[0]
        dirname = directory.split('/')[1]
        if base_directory:
            try:
                parent_dir = self.directory_contents(base_directory)
                workspace_directories = []
                for dir in parent_dir:
                    workspace_directories.append(dir['name'])
                # Directory should exist before deleting
                assert dirname in workspace_directories, FileSystemException
            except FileSystemException:
                pass
        else:   # This directory exist under /root
            try:
                parent_dir = self.directory_contents('/')['content']
                workspace_directories = []
                for dir in parent_dir:
                    workspace_directories.append(dir['name'])
                # Directory should exist before deleting
                assert dirname in workspace_directories, FileSystemException
            except FileSystemException:
                pass
        command = f'{self._route}{directory}'
        data = {
            'path': directory,
        }
        try:
            type = RequestType.DELETE
            delete_to_filesystem = CommandManager(self._user).createRunCommand(
                command=command,
                type=type,
                data=json_encode(data))
            return delete_to_filesystem
        except FileSystemException:
            pass


class SessionInitializationException(Exception):
    def __init__(self):
        print('[SessionInitializationException]:    The session has failed to initialize.')


class SessionTerminationException(Exception):
    def __init__(self):
        print('[SessionTerminationException]:       The session failed to terminate.')


class SessionTypes:
    NOTEBOOK = 'notebook'
    CONSOLE  = 'console'
    FILE     = 'file'


class SessionManager:
    ''' 
        Session management object
            @param: user (User)
    '''
    def __init__(self, user: User, type: SessionTypes, path: str = None):
        self._route = 'api/sessions'
        self._user = user
        self._session_id = self._createSession(user, type=type, path=path)
    
    def _createSession(self, user: User, type: SessionTypes, path: str = None):
        command = self._route
        data = {
            'name': str(user.id),
            'path': f'{path}',
            'type': type
        }
        try:
            post_to_sessions = CommandManager(
                self._user).createRunCommand(
                    command=command,
                    type=RequestType.POST,
                    data=json_encode(data))
            # self._kernel_id = post_to_sessions['kernel']['id']
            return post_to_sessions['id']
        except SessionInitializationException:
            pass


    def createWebsocket(self):
        ''' 
            Creates a websocket connection to specific session
        '''
        log = logging.getLogger('KernelClient')
        try:
            return KernelClient(
                HTTP_API_ENDPOINT,
                WS_API_ENDPOINT,
                self._session_id,
                timeout=60,
                logger=log)
        except WebsocketIntitializationException:
            pass


    def __del__(self):
        command = f'{self._route}/{self._session_id}'
        try:
            CommandManager(self._user).createRunCommand(
                command=command,
                type=RequestType.DELETE)
        except SessionTerminationException:
            pass



def __main__():
    description='''
        This function accepts the path to the local .py script
        which will be run on the cloud (and not saved on the cloud). 
        If the scripts returns an output, the output will be printed to the terminal.

        Usage:

        send --file ='<path to your local .py file>'
        send -f     ='<path to your local .py file>'
    '''
    argv = sys.argv[1:]
    try:
        options, args = getopt.getopt(argv, "f:", ["file="])
    except:
        print(f'File Input Error \n {description}')
    for name, value in options:
        if name in ['-f', '--file']:
            run_local_file(value)
