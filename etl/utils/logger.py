import logging


logger = logging.getLogger("logger")

handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(fmt='[%(asctime)s] [%(levelname)s] %(message)s', datefmt="%a, %d %b %Y %H:%M:%S")
handler.setFormatter(formatter)

logger.addHandler(handler)