import sys

from .cli.bridge import main

try:
    main(sys.argv[1])
except IndexError:
    main()
