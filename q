(venv) D:\Joe\Develop\GrafanaInfluxdb\Autoimport>python ews_fetch.py
Traceback (most recent call last):
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\ews_fetch.py", line 83, in <module>
    main()
    ~~~~^^
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\ews_fetch.py", line 51, in main
    processed_folder = ensure_folder(account.inbox, PROCESSED_PATH)
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\ews_fetch.py", line 27, in ensure_folder
    folder / name  # 觸發層級瀏覽
    ~~~~~~~^~~~~~
  File "D:\Joe\Develop\GrafanaInfluxdb\Autoimport\venv\Lib\site-packages\exchangelib\folders\base.py", line 803, in __truediv__
    raise ErrorFolderNotFound(f"No subfolder with name {other!r}")
exchangelib.errors.ErrorFolderNotFound: No subfolder with name 'P'

(venv) D:\Joe\Develop\GrafanaInfluxdb\Autoimport>
