import sys
import dataconf

from app.config import Config
from app.dumper import Dumper


def read_arguments(arguments):
    try:
        if len(arguments) > 1:
            congig_file = arguments[1]
            config = dataconf.load(congig_file, Config)
            return config
        else:
            raise Exception("Le fichier de configutation est requis")
    except Exception as ex:
        print(ex)


def main():
    try:
        config = read_arguments(sys.argv)
        dumper = Dumper(config)
        dumper.start()
    except Exception as ex:
        print(ex)


if __name__ == '__main__':
    main()


