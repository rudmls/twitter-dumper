import re


def remove_link(expression):
    return re.sub(r'https?://.*[\r\n]*', '', expression, flags=re.MULTILINE)
