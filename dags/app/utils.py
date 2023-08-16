def normalize(raw_name: str):
    try:
        return raw_name.split('"')[1].lower()
    except IndexError:
        raw_name.lower()
