#!/usr/bin/env bash
action() {
    local origin="$( /bin/pwd )"
    [ -z "$USER" ] && export USER="$( whoami )"
    #
    # global variables
    #
    export LD_LIBRARY_PATH=
    export PYTHONPATH=
    export BTV_BASE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && /bin/pwd )"
    # check where we are
    case "$( hostname -f )" in
        vispa-*|vispa-*.physik.rwth-aachen.de)
        mkdir -p "$HOME/.config/dask"
        [ -e "$HOME/.config/dask/vispa.yml" ] || ln -s "$BTV_BASE/dask.yml" "$HOME/.config/dask/vispa.yml"
        export BTV_DATA="/net/scratch/cms/btv"
        export BTV_DASK_CERT="$BTV_DATA/tls_ca_file.pem"
        export BTV_DASK_KEY="$BTV_DATA/tls_client_key.pem"
        export DASGOCLIENT="$(ls -1t /cvmfs/cms.cern.ch/cc?_amd64_*/cms/dasgoclient/*/bin/dasgoclient  | head -n 1)"
        software conda
        ulimit -f hard
        ;;
    esac
    # complain when required variables are not set
    true ${USER:?user name environment variable missing}
    true ${BTV_DATA:?data directory environment variable missing}
    # set defaults of other variables
    [ -z "$BTV_SOFTWARE" ] && export BTV_SOFTWARE="$BTV_DATA/software/$USER/$BTV_DIST_VERSION"
    [ -z "$BTV_STORE" ] && export BTV_STORE="$BTV_DATA/store"
    #
    # law setup
    #
    # law and luigi setup
    export LAW_HOME="$BTV_BASE/.law"
    export LAW_CONFIG_FILE="$BTV_BASE/law.cfg"
    # print a warning when no luigi scheduler host is set
    if [ -z "$BTV_SCHEDULER" ]; then
        2>&1 echo "NOTE: BTV_SCHEDULER is not set, use '--local-scheduler' in your tasks!"
    fi
    #
    # software setup
    #
    btv_install_software() {
        local mode="$1"
        # conda unavailable?
        if ! command -v conda &>/dev/null
        then
            echo "Conda not found" >&2
            exit 1
        fi
        conda env update && conda activate btv && return "0"
    }
    export -f btv_install_software
    btv_install_software silent
    # add _this_ repo
    export PYTHONPATH="$BTV_BASE:$PYTHONPATH"
    export GLOBUS_THREAD_MODEL="none"
    # source law's bash completion scipt
    source "$( law completion )"
}
action "$@"
