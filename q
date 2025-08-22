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
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\version.py", line 202, in guess
    list(ConvertId(protocol=protocol).call([AlternateId(id="DUMMY", format=EWS_ID, mailbox="DUMMY")], ENTRY_ID))
    ~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\services\common.py", line 216, in _elems_to_objs
    for elem in elems:
                ^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\services\common.py", line 279, in _chunked_get_elements
    yield from self._get_elements(payload=payload_func(chunk, **kwargs))
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
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\util.py", line 866, in post_ratelimited
    protocol.retry_policy.raise_response_errors(r)
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\protocol.py", line 727, in raise_response_errors
    raise UnauthorizedError(f"Invalid credentials for {response.url}")
exchangelib.errors.UnauthorizedError: Invalid credentials for https://webmail.linebank.com.tw/EWS/Exchange.asmx
