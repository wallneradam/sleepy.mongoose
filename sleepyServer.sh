#!/bin/bash

### Paramaters ###

ADDRESS=localhost
PORT=27080

##################


# Action is the 1st parameter
action=$1
shift

python="`which python`"
dir="$(cd `dirname "$0"` && pwd)"
server="$dir/httpd.py"


# Process action
case ${action} in
    # Start foreground mode with debug console
    debug)
        # Run it in foreground
        "${python}" "${server}" $@
        ;;

    start)
        # Check if running
        pgrep -f "${server}" >/dev/null
        if [ $? -eq 0 ]; then
            echo "The server is already running.";
            exit 1;
        fi

        # Run it in background
        "${python}" "${server}" $@ &>/dev/null &

        # Check if the server is running (max 10s to wait)
        for i in 1 2 3 4 5; do
            sleep 1
            wget -qO - ${ADDRESS}:${PORT}/_hello >/dev/null
            res=$?
            if [ ${res} -eq 0 ]; then
                echo "The server has started successfully."
                exit 0;
            fi
        done
        killall ${server}
        echo "The server has not started!"
        exit 1;
        ;;

    stop)
        # Check if running
        pgrep -f "${server}" >/dev/null
        if [ $? -ne 0 ]; then
            echo "The server is not running.";
            exit 1;
        fi
        pkill -SIGINT -f "${server}" >/dev/null

        pgrep -f "${server}" >/dev/null
        res=$?

        if [ ${res} -eq 0 ]; then
            echo -n "Waiting for server to stop...  "
            for i in 5 4 3 2 1; do
                echo -n -e "\b$i"
                sleep 1
                # If it is still running
                pgrep -f "${server}" >/dev/null
                res=$?
                if [ ${res} -ne 0 ]; then break; fi
            done

            if [ ${res} -eq 0 ]; then
                echo -e "\b Force kill server..."
                pkill -f "${server}" >/dev/null
            else
                echo -e "\b\bOK"
            fi
        fi

        pgrep -f "${server}" >/dev/null
        res=$?

        if [ ${res} -ne 0 ]; then
            echo "The server stopped successfully."
        else
            echo "Some errors occured while stopping the server! Exit code: $res";
            exit ${res}
        fi
        ;;
    test)
        wget -qO - ${ADDRESS}:${PORT}/_hello
        echo ""
        ;;
    *)
        echo "Usage: `basename "$0"` start|stop|debug|test"
        ;;
esac
