
(venv) D:\Joe\Develop\GrafanaInfluxdb\Autoimport>python ews_fetch.py
Traceback (most recent call last):
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\ews_fetch.py", line 85, in <module>
    main()
    ~~~~^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\ews_fetch.py", line 56, in main
    for item in qs.only("subject", "attachments", "datetime_received"):
                ~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\queryset.py", line 270, in __iter__
    yield from self._format_items(items=self._query(), return_format=self.return_format)
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\queryset.py", line 345, in _item_yielder
    for i in iterable:
             ^^^^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\account.py", line 737, in fetch
    yield from self._consume_item_service(
    ...<7 lines>...
    )
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\account.py", line 426, in _consume_item_service
    is_empty, items = peek(items)
                      ~~~~^^^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\util.py", line 152, in peek
    first = next(iterable)
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\folders\collections.py", line 211, in find_items
    yield from FindItem(account=self.account, page_size=page_size).call(
    ...<10 lines>...
    )
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\services\common.py", line 216, in _elems_to_objs
    for elem in elems:
                ^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\services\common.py", line 801, in _paged_call
    pages = self._get_pages(payload_func, kwargs, len(paging_infos))
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\services\common.py", line 897, in _get_pages
    payload = payload_func(**kwargs)
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\services\find_item.py", line 106, in get_payload
    payload.append(restriction.to_xml(version=self.account.version))
                   ~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\restriction.py", line 564, in to_xml
    return self.q.to_xml(folders=self.folders, version=version, applies_to=self.applies_to)
           ~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\restriction.py", line 358, in to_xml
    elem = self.xml_elem(folders=folders, version=version, applies_to=applies_to)
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\restriction.py", line 479, in xml_elem
    elem.append(c.xml_elem(folders=folders, version=version, applies_to=applies_to))
                ~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\restriction.py", line 479, in xml_elem
    elem.append(c.xml_elem(folders=folders, version=version, applies_to=applies_to))
                ~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\restriction.py", line 453, in xml_elem
    field_path = self._get_field_path(folders, applies_to=applies_to, version=version)
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\restriction.py", line 426, in _get_field_path
    raise InvalidField(f"Unknown field path {self.field_path!r} on folders {folders}")
exchangelib.fields.InvalidField: Unknown field path 'sender__email_address' on folders (Inbox(Root(<exchangelib.account.Account object at 0x0000023911519A90>, '[self]', 'root', 14, 0, 63, None, 'AAMkADViNTdiZGZmLTM0MTMtNGE4MC05YTVmLWJiMDFmNjdlMTRiMwAuAAAAAAB1FgzYM8D4Qo2HEfEffNL6AQDrsUVXp+/hSJY20/MkImfkAAAAAAEBAAA=', 'AQAAABYAAADrsUVXp+/hSJY20/MkImfkAAM3tWLz'), '收件匣', 31513, 336, 12, 'IPF.Note', 'AAMkADViNTdiZGZmLTM0MTMtNGE4MC05YTVmLWJiMDFmNjdlMTRiMwAuAAAAAAB1FgzYM8D4Qo2HEfEffNL6AQDrsUVXp+/hSJY20/MkImfkAAAAAAEMAAA=', 'AQAAABQAAAB7EVRe8wnGT77sapSzdsUfAAeQGg=='),)
