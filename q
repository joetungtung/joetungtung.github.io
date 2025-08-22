
(venv) D:\Joe\Develop\GrafanaInfluxdb\Autoimport>python ews_fetch.py
Traceback (most recent call last):
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\cached_property.py", line 63, in __get__
    return obj_dict[name]
           ~~~~~~~~^^^^^^
KeyError: 'inbox'

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\cached_property.py", line 63, in __get__
    return obj_dict[name]
           ~~~~~~~~^^^^^^
KeyError: 'root'

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\urllib3\connection.py", line 198, in _new_conn
    sock = connection.create_connection(
        (self._dns_host, self.port),
    ...<2 lines>...
        socket_options=self.socket_options,
    )
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\urllib3\util\connection.py", line 60, in create_connection
    for res in socket.getaddrinfo(host, port, family, socket.SOCK_STREAM):
               ~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Joe\Develop\Python\Lib\socket.py", line 977, in getaddrinfo
    for res in _socket.getaddrinfo(host, port, family, type, proto, flags):
               ~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
socket.gaierror: [Errno 11001] getaddrinfo failed

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\urllib3\connectionpool.py", line 787, in urlopen
    response = self._make_request(
        conn,
    ...<10 lines>...
        **response_kw,
    )
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\urllib3\connectionpool.py", line 488, in _make_request
    raise new_e
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\urllib3\connectionpool.py", line 464, in _make_request
    self._validate_conn(conn)
    ~~~~~~~~~~~~~~~~~~~^^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\urllib3\connectionpool.py", line 1093, in _validate_conn
    conn.connect()
    ~~~~~~~~~~~~^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\urllib3\connection.py", line 753, in connect
    self.sock = sock = self._new_conn()
                       ~~~~~~~~~~~~~~^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\urllib3\connection.py", line 205, in _new_conn
    raise NameResolutionError(self.host, self, e) from e
urllib3.exceptions.NameResolutionError: <urllib3.connection.HTTPSConnection object at 0x000001F9A717B8C0>: Failed to resolve 'https' ([Errno 11001] getaddrinfo failed)

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\requests\adapters.py", line 644, in send
    resp = conn.urlopen(
        method=request.method,
    ...<9 lines>...
        chunked=chunked,
    )
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\urllib3\connectionpool.py", line 841, in urlopen
    retries = retries.increment(
        method, url, error=new_e, _pool=self, _stacktrace=sys.exc_info()[2]
    )
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\urllib3\util\retry.py", line 519, in increment
    raise MaxRetryError(_pool, url, reason) from reason  # type: ignore[arg-type]
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
urllib3.exceptions.MaxRetryError: HTTPSConnectionPool(host='https', port=443): Max retries exceeded with url: /webmail.linebank.com.tw/EWS/Exchange.asmx/EWS/Exchange.asmx (Caused by NameResolutionError("<urllib3.connection.HTTPSConnection object at 0x000001F9A717B8C0>: Failed to resolve 'https' ([Errno 11001] getaddrinfo failed)"))

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\util.py", line 825, in post_ratelimited
    r = session.post(**kwargs)
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\requests\sessions.py", line 637, in post
    return self.request("POST", url, data=data, json=json, **kwargs)
           ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\requests\sessions.py", line 589, in request
    resp = self.send(prep, **send_kwargs)
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\requests\sessions.py", line 703, in send
    r = adapter.send(request, **kwargs)
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\requests\adapters.py", line 677, in send
    raise ConnectionError(e, request=request)
requests.exceptions.ConnectionError: HTTPSConnectionPool(host='https', port=443): Max retries exceeded with url: /webmail.linebank.com.tw/EWS/Exchange.asmx/EWS/Exchange.asmx (Caused by NameResolutionError("<urllib3.connection.HTTPSConnection object at 0x000001F9A717B8C0>: Failed to resolve 'https' ([Errno 11001] getaddrinfo failed)"))

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\version.py", line 202, in guess
    list(ConvertId(protocol=protocol).call([AlternateId(id="DUMMY", format=EWS_ID, mailbox="DUMMY")], ENTRY_ID))
    ~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\services\common.py", line 216, in _elems_to_objs
    for elem in elems:
                ^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\services\common.py", line 279, in _chunked_get_elements
    yield from self._get_elements(payload=payload_func(chunk, **kwargs))
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\services\common.py", line 327, in _get_elements
    raise e
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\services\common.py", line 300, in _get_elements
    yield from self._response_generator(payload=payload)
               ~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\services\common.py", line 263, in _response_generator
    response = self._get_response_xml(payload=payload)
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\services\common.py", line 396, in _get_response_xml
    r = self._get_response(payload=payload, api_version=api_version)
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\services\common.py", line 347, in _get_response
    r, session = post_ratelimited(
                 ~~~~~~~~~~~~~~~~^
        protocol=self.protocol,
        ^^^^^^^^^^^^^^^^^^^^^^^
    ...<8 lines>...
        timeout=self.timeout or self.protocol.TIMEOUT,
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\util.py", line 833, in post_ratelimited
    raise ErrorTimeoutExpired(f"Reraised from {e.__class__.__name__}({e})")
exchangelib.errors.ErrorTimeoutExpired: Reraised from ConnectionError(HTTPSConnectionPool(host='https', port=443): Max retries exceeded with url: /webmail.linebank.com.tw/EWS/Exchange.asmx/EWS/Exchange.asmx (Caused by NameResolutionError("<urllib3.connection.HTTPSConnection object at 0x000001F9A717B8C0>: Failed to resolve 'https' ([Errno 11001] getaddrinfo failed)")))

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\ews_fetch.py", line 83, in <module>
    main()
    ~~~~^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\ews_fetch.py", line 44, in main
    inbox: Inbox = account.inbox
                   ^^^^^^^^^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\cached_property.py", line 67, in __get__
    return obj_dict.setdefault(name, self.func(obj))
                                     ~~~~~~~~~^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\account.py", line 303, in inbox
    return self.root.get_default_folder(Inbox)
           ^^^^^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\cached_property.py", line 67, in __get__
    return obj_dict.setdefault(name, self.func(obj))
                                     ~~~~~~~~~^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\account.py", line 367, in root
    return Root.get_distinguished(account=self)
           ~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\folders\roots.py", line 145, in get_distinguished
    return cls._get_distinguished(
           ~~~~~~~~~~~~~~~~~~~~~~^
        folder=cls(
        ^^^^^^^^^^^
    ...<5 lines>...
        )
        ^
    )
    ^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\folders\base.py", line 226, in _get_distinguished
    return cls.resolve(account=folder.account, folder=folder)
           ~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\folders\base.py", line 530, in resolve
    folders = list(FolderCollection(account=account, folders=[folder]).resolve())
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\folders\collections.py", line 334, in resolve
    additional_fields = self.get_folder_fields(target_cls=self._get_target_cls())
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\folders\collections.py", line 282, in get_folder_fields
    for f in target_cls.supported_fields(version=self.account.version)
                                                 ^^^^^^^^^^^^^^^^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\account.py", line 221, in version
    self._version = self.protocol.version.copy()
                    ^^^^^^^^^^^^^^^^^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\protocol.py", line 480, in version
    self.config.version = Version.guess(self, api_version_hint=self.api_version_hint)
                          ~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\version.py", line 206, in guess
    raise TransportError(f"No valid version headers found in response ({e!r})")
exchangelib.errors.TransportError: No valid version headers found in response (ErrorTimeoutExpired('Reraised from ConnectionError(HTTPSConnectionPool(host=\'https\', port=443): Max retries exceeded with url: /webmail.linebank.com.tw/EWS/Exchange.asmx/EWS/Exchange.asmx (Caused by NameResolutionError("<urllib3.connection.HTTPSConnection object at 0x000001F9A717B8C0>: Failed to resolve \'https\' ([Errno 11001] getaddrinfo failed)")))'))
