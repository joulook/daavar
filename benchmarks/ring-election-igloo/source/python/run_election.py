import sys
from leader.election import mainLoop

def main():
    OUT_HOST = sys.argv[1]
    MY_ID = int(sys.argv[2])
    IN_PORT = int(sys.argv[3])
    OUT_PORT = int(sys.argv[4])
    mainLoop((OUT_HOST, OUT_PORT), MY_ID, IN_PORT)


if __name__ == '__main__':
    main()
