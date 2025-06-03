# encoding: utf-8
#
# Copyright (c) 2023 Glencoe Software, Inc. All rights reserved.
#
# This software is distributed under the terms described by the LICENCE file
# you can find at the root of the distribution bundle.
# If the file is missing please request a copy by contacting
# support@glencoesoftware.com.
import atexit
import getpass
import importlib.util
import logging
import weakref

import omero
from omero.clients import BaseClient
from omero.gateway import BlitzGateway

LOGGER = logging.getLogger(__name__)
ACTIVE_CONNECTORS = weakref.WeakSet()


class OMEROConnection:
    """
    A class designed to handle OMERO connections and manage sessions gracefully
    """
    def __init__(self, client=None, server=None, port=4064, username=None,
                 password=None, session_key=None, allow_token=True):
        """
        :param client: An existing omero.client object to be used instead of
        creating a new connection.
        :param server: server address to connect to
        :param port: port number of server (default 4064)
        :param username: username for server login
        :param password: password for server login
        :param session_key: key to join an existing session,
        supersedes username/password
        :param allow_token: True/False Search for omero_user_token before
        trying to use credentials. Default True.
        """
        self.client = None
        self.gateway = None
        self.external_client = client is not None
        if isinstance(client, OMEROConnection):
            raise TypeError("Client for creating an OMEROConnection cannot be"
                            " an existing existing OMEROConnection")
        elif isinstance(client, BlitzGateway):
            # Unwrap BlitzGateway
            self.gateway = client
            self.client = client.c
        elif isinstance(client, BaseClient):
            # Use existing client
            self.client = client
        elif client:
            raise TypeError(f"Invalid client type {type(client)}")
        self.session = None
        self.temp_session = False
        if self.client is not None:
            # Infer details from client, fallback to params
            self.server = self.client.getProperty("omero.host")
            if server and self.server != server:
                LOGGER.warning(f"Host already set to '{self.server}' in "
                               f"provided client, param will be ignored")
            elif server and not self.server:
                self.server = server
            self.port = self.client.getProperty("omero.port") or port
            if not self.server:
                LOGGER.error("Unknown host for provided client")
        else:
            self.server = server
            self.port = port
        self.username = username
        self.password = password
        self.session_key = session_key
        if allow_token and self.need_connection_details():
            self.get_user_token()
        # Make sure that session closer runs on interpreter exit.
        ACTIVE_CONNECTORS.add(self)

    def __getattr__(self, attr):
        # Forward function calls to the client object, if one exists.
        if self.client is None:
            raise ValueError("Client is not initialised")
        return getattr(self.client, attr)

    def __enter__(self):
        # Context manager, create a session if one doesn't exist.
        if self.client is None:
            LOGGER.debug("Creating temporary OMERO session")
            connected = self.connect(interactive=False)
            if connected:
                self.temp_session = True
            else:
                raise Exception("Failed to connect to OMERO")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.client is not None and self.temp_session:
            LOGGER.debug("Exiting temporary OMERO session")
            self.shutdown()
            self.temp_session = False

    def shutdown(self):
        # Close any sessions that were opened
        self.temp_session = False
        if self.client is not None:
            LOGGER.debug("Closing OMERO session")
            self.client.closeSession()
            self.client = None
            self.gateway = None
            self.session = None

    def __del__(self):
        # Make sure we close sessions on deletion.
        if not self.external_client:
            self.shutdown()

    @property
    def connected(self):
        return self.client is not None

    def need_connection_details(self):
        # Do we have enough info to try connecting?
        if self.client is not None:
            return False
        if self.server is None:
            return True
        if (self.username is None or self.password is None) and \
                self.session_key is None:
            return True
        return False

    def connect(self, interactive=True, keep_alive=True):
        if self.connected:
            return True
        # Attempt to establish a connection.
        if self.need_connection_details():
            if interactive:
                if detect_jupyter():
                    self.connect_widget()
                else:
                    self.connect_cli()
                return False
            else:
                raise Exception("Insufficient details to create a connection")

        self.client = omero.client(host=self.server, port=self.port)
        if self.session_key is not None:
            try:
                self.client.joinSession(self.session_key)
                self.session = self.client.getSession()
                self.session.detachOnDestroy()
            except Exception as e:
                print(f"Failed to join session, token may have expired: {e}")
                self.client = None
                self.session = None
                return False
        elif self.username is not None:
            try:
                self.session = self.client.createSession(
                    username=self.username, password=self.password)
            except Exception as e:
                print(f"Failed to create session: {e}")
                self.client = None
                self.session = None
                return False
        else:
            self.client = None
            self.session = None
            raise Exception(
                "Not enough details to create a server connection.")
        print(f"Connected to {self.server}")
        if keep_alive:
            self.client.enableKeepAlive(60)
        return True

    def connect_widget(self):
        # Prompt for connection details using a Jupyter widget.
        from ipywidgets import widgets
        from IPython.core.display_functions import display
        login_button = widgets.Button(description="Connect")
        prompt_server = widgets.Text(
            value=self.server or '',
            placeholder='Enter server hostname',
            description='Address:',
            disabled=False)
        prompt_port = widgets.IntText(
            value=self.port or 4064,
            placeholder='Enter server port',
            description='Port:',
            disabled=False)
        prompt_user = widgets.Text(
            value='',
            placeholder=self.username or '',
            description='Username:',
            disabled=False)
        prompt_pass = widgets.Password(
            value=self.password or '',
            placeholder='Enter login password',
            description='Password:',
            disabled=False)

        items = [
            widgets.Label(value="Connect to OMERO Server"),
            prompt_server,
            prompt_port,
            prompt_user,
            prompt_pass,
            login_button,
        ]

        prompt = widgets.VBox(items)
        output = widgets.Output()

        def login_funct(e):
            with output:
                # We use print statements here since everything goes into
                # the Jupyter console.
                self.server = prompt_server.get_interact_value() or None
                self.port = prompt_port.get_interact_value() or None
                self.username = prompt_user.get_interact_value() or None
                self.password = prompt_pass.get_interact_value() or None
                print(f"Connecting to {self.server}")
                if self.need_connection_details():
                    print("Insufficient connection details")
                else:
                    for widget in items:
                        widget.disabled = True
                    try:
                        success = self.connect(interactive=False)
                    except Exception as exc:
                        print(f"Error encountered during connection: {exc}")
                        success = False
                    if success:
                        prompt.close()
                    else:
                        print("Failed to connect, please try again.")
                        for widget in items:
                            widget.disabled = False

        login_button.on_click(login_funct)

        display(prompt, output)

    def connect_cli(self):
        # Prompt for login details using the console
        print("Enter server credentials...")
        new_server = input(f"Enter server address [Current={self.server}]: ")
        if new_server:
            self.server = new_server
        new_port = input(f"Enter server port [Current={self.port}]: ")
        if new_port and new_port.isnumeric():
            self.port = int(new_port)
        new_user = input(f"Enter username [Current={self.username}]: ")
        if new_user:
            self.username = new_user
        new_pass = getpass.getpass(prompt="Enter password: ")
        if new_pass:
            self.password = new_pass

        success = self.connect(interactive=False)
        if success:
            print("Connection successful")
        else:
            print("Unable to connect")

    def get_user_token(self):
        # Check for user token and apply to class
        if self.server is None and self.username is None:
            try:
                import omero_user_token
                LOGGER.info("Requesting token info")
                token = omero_user_token.get_token()
                self.server, port = token[token.find('@') + 1:].split(':')
                self.port = int(port)
                self.session_key = token[:token.find('@')]
                LOGGER.info(f"Found token for connection to"
                            f" {self.server}:{self.port}")
            except ImportError:
                LOGGER.info("omero_user_token not installed")
            except AttributeError:
                LOGGER.warning("Please update omero-user-token to >=0.3.0")
            except Exception:
                LOGGER.error("Failed to process user token", exc_info=True)
        else:
            LOGGER.info("Server or User details already provided, "
                        "will skip token check.")

    def get_gateway(self):
        if self.client is None:
            raise Exception("Client connection not initialised")
        if self.gateway is None:
            LOGGER.debug("Constructing BlitzGateway")
            self.gateway = BlitzGateway(client_obj=self.client)
        return self.gateway

    def get_client(self):
        if self.client is None:
            LOGGER.warning("Client connection not initialised")
        return self.client


def detect_jupyter():
    # Determine whether we're running in a Jupyter Notebook.
    try:
        from IPython import get_ipython
    except ImportError:
        return False
    shell = get_ipython().__class__.__name__
    if shell == 'ZMQInteractiveShell':
        if importlib.util.find_spec("ipywidgets") is None:
            LOGGER.warning(
                "Detected Jupyter environment but no ipywidgets, "
                "cannot show interactive login widget.")
            return False
        LOGGER.debug("Detected Jupyter environment")
        return True
    return False


def get_connection(client=None, **kwargs):
    """Create an OMEROConnection instance or use existing if supplied"""
    if client is not None and isinstance(client, OMEROConnection):
        return client
    return OMEROConnection(client=client, **kwargs)


def cleanup_sessions():
    # Shut down any active sessions when exiting Python
    for connector in ACTIVE_CONNECTORS:
        connector.shutdown()


atexit.register(cleanup_sessions)
