from cgi import parse_header

def decode_body(content_type, body_data, default_encoding='UTF-8'):
    """
    Decode a given reponse body.
    """
    # Ideally, encoding should be in the content type
    (ct, ctopts) = parse_header(content_type)
    encoding = ctopts.get('charset', default_encoding)

    # Return the decoded payload along with the content-type.
    return (ct, ctopts, body_data.decode(encoding))
