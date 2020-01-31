import tempfile
import os

def generate_ssl_object(module, ssl_cafile, ssl_certfile, ssl_keyfile,
                        ssl_crlfile=None):
    """
    Generates a dict object that is used when dealing with ssl connection.
    When values given are file content, it takes care of temp file creation.
    """

    ssl_files = {
        'cafile': {'path': ssl_cafile, 'is_temp': False},
        'certfile': {'path': ssl_certfile, 'is_temp': False},
        'keyfile': {'path': ssl_keyfile, 'is_temp': False},
        'crlfile': {'path': ssl_crlfile, 'is_temp': False}
    }

    for key, value in ssl_files.items():
        if value['path'] is not None:
            # TODO is that condition sufficient?
            if value['path'].startswith("-----BEGIN"):
                # value is a content, need to create a tempfile
                fd, path = tempfile.mkstemp(prefix=key)
                with os.fdopen(fd, 'w') as tmp:
                    tmp.write(value['path'])
                ssl_files[key]['path'] = path
                ssl_files[key]['is_temp'] = True
            elif not os.path.exists(os.path.dirname(value['path'])):
                # value is not a content, but path does not exist,
                # fails the module
                module.fail_json(
                    msg='\'%s\' is not a content and provided path does not '
                        'exist, please check your SSL configuration.' % key
                )

    return ssl_files