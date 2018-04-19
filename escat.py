#!/usr/bin/python3
import argparse

from elasticsearch import Elasticsearch


class ESAction(argparse.Action):
    """
    argparse.Action which takes the provided url, saves it, and also instantiates an Elasticsearch client with it.
    """
    def __call__(self,
                 parser: argparse.ArgumentParser,
                 namespace: argparse.Namespace,
                 values,
                 option_string=None) -> None:
        setattr(namespace, self.dest, values)
        setattr(namespace, "esclient", Elasticsearch(values))


class BytesOption(argparse._StoreAction):
    """
    """
    def __init__(self,
                 **kwargs):
        # TODO: Add choices for this or just let people do what they want (in other words, what elasticsearch does)?
        kwargs["option_strings"].append("--bytes")
        kwargs["dest"] = "bytes"
        kwargs["type"] = str
        kwargs["help"] = "change cat API bytes units to the specified units"
        super(BytesOption, self).__init__(**kwargs)


class SizeOption(argparse._StoreAction):
    """
    """
    def __init__(self,
                 **kwargs):
        # TODO: Add choices for this or just let people do what they want (in other words, what elasticsearch does)?
        kwargs["option_strings"].append("--size")
        kwargs["dest"] = "size"
        kwargs["type"] = str
        kwargs["help"] = "change cat API size multiplier in which to display values"
        super(SizeOption, self).__init__(**kwargs)


class HealthOption(argparse._StoreAction):
    """
    """
    def __init__(self,
                 **kwargs):
        kwargs["option_strings"].append("--health")
        kwargs["dest"] = "health"
        kwargs["type"] = str
        kwargs["help"] = "health status (green, yellow, or red) to filter by"
        super(HealthOption, self).__init__(**kwargs)


class LocalOption(argparse._StoreTrueAction):
    """
    """
    def __init__(self,
                 **kwargs):
        kwargs["option_strings"].append("--local")
        kwargs["dest"] = "local"
        kwargs["help"] = "return local information; do not retrieve the state from master node"
        super(LocalOption, self).__init__(**kwargs)


class PrimaryOption(argparse._StoreTrueAction):
    """
    """
    def __init__(self,
                 **kwargs):
        kwargs["option_strings"].append("--primary")
        kwargs["dest"] = "primary"
        kwargs["help"] = "return stats only for primary shards"
        super(PrimaryOption, self).__init__(**kwargs)


class MasterTimeoutOption(argparse._StoreAction):
    """
    """
    # TODO: Make sure this works correctly...
    def __init__(self,
                 **kwargs):
        kwargs["option_strings"].append("--master_timeout")
        kwargs["dest"] = "master_timeout"
        kwargs["type"] = str
        kwargs["help"] = "explicit operation timeout for connection to master node"
        super(MasterTimeoutOption, self).__init__(**kwargs)


def catgeneric(args: argparse.Namespace) -> str:
    return args.cmd(
        args,
        format=args.format,
        h=args.columns,
        help=args.information,
        s=args.sort,
        v=args.verbose
    )


def aliases(args: argparse.Namespace,
            **kwargs) -> str:
    return args.esclient.cat.aliases(
        name=args.name,
        local=args.local,
        master_timeout=args.master_timeout,
        **kwargs
    )


def allocation(args: argparse.Namespace,
               **kwargs) -> str:
    return args.esclient.cat.allocation(
        node_id=args.node_id,
        bytes=args.bytes,
        local=args.local,
        master_timeout=args.master_timeout,
        **kwargs
    )


def count(args: argparse.Namespace,
          **kwargs) -> str:
    return args.esclient.cat.count(
        index=args.index,
        master_timeout=args.master_timeout,
        **kwargs
    )


def fielddata(args: argparse.Namespace,
              **kwargs) -> str:
    return args.esclient.cat.fielddata(
        fields=args.field,
        bytes=args.bytes,
        master_timeout=args.master_timeout,
        **kwargs
    )


def health(args: argparse.Namespace,
           **kwargs) -> str:
    return args.esclient.cat.health(
        ts=args.ts,
        master_timeout=args.master_timeout,
        **kwargs
    )


def indices(args: argparse.Namespace,
            **kwargs) -> str:
    return args.esclient.cat.indices(
        index=args.index,
        bytes=args.bytes,
        health=args.health,
        local=args.local,
        master_timeout=args.master_timeout,
        pri=args.primary,
        **kwargs
    )


def master(args: argparse.Namespace,
           **kwargs) -> str:
    return args.esclient.cat.master(
        local=args.local,
        master_timeout=args.master_timeout,
        **kwargs
    )


def nodeattrs(args: argparse.Namespace,
              **kwargs) -> str:
    return args.esclient.cat.nodeattrs(
        local=args.local,
        master_timeout=args.master_timeout,
        **kwargs
    )


def nodes(args: argparse.Namespace,
          **kwargs) -> str:
    return args.esclient.cat.nodes(
        full_id=args.full_id,
        local=args.local,
        master_timeout=args.master_timeout,
        **kwargs
    )


def pending_tasks(args: argparse.Namespace,
                  **kwargs) -> str:
    return args.esclient.cat.pending_tasks(
        local=args.local,
        master_timeout=args.master_timeout,
        **kwargs
    )


def plugins(args: argparse.Namespace,
            **kwargs) -> str:
    return args.esclient.cat.plugins(
        local=args.local,
        master_timeout=args.master_timeout,
        **kwargs
    )


def recovery(args: argparse.Namespace,
             **kwargs) -> str:
    return args.esclient.cat.recovery(
        index=args.index,
        bytes=args.bytes,
        master_timeout=args.master_timeout,
        **kwargs
    )


def repositories(args: argparse.Namespace,
                 **kwargs) -> str:
    return args.esclient.cat.repositories(
        local=args.local,
        master_timeout=args.master_timeout,
        **kwargs
    )


def segments(args: argparse.Namespace,
             **kwargs) -> str:
    return args.esclient.cat.segments(
        index=args.index,
        bytes=args.bytes,
        **kwargs
    )


def shards(args: argparse.Namespace,
           **kwargs) -> str:
    return args.esclient.cat.shards(
        index=args.index,
        bytes=args.bytes,
        local=args.local,
        master_timeout=args.master_timeout,
        **kwargs
    )


def snapshots(args: argparse.Namespace,
              **kwargs) -> str:
    return args.esclient.cat.snapshots(
        repository=args.repository,
        ignore_unavailable=args.ignore_unavailable,
        master_timeout=args.master_timeout,
        **kwargs
    )


def templates(args: argparse.Namespace,
              **kwargs) -> str:
    return args.esclient.cat.templates(
        name=args.name,
        local=args.local,
        master_timeout=args.master_timeout,
        **kwargs
    )


def thread_pool(args: argparse.Namespace,
                **kwargs) -> str:
    return args.esclient.cat.thread_pool(
        thread_pool_patterns=args.pattern,
        local=args.local,
        master_timeout=args.master_timeout,
        size=args.size,
        **kwargs
    )


if __name__ == "__main__":
    ################################################################################
    # Initialize main parser and subparsers
    ################################################################################
    parser = argparse.ArgumentParser(description="Replacement for es2unix, based on elasticsearch cat API.")
    subparsers = parser.add_subparsers(title="cat API commands", help="valid cat API commands", dest="command")
    subparsers.required = True

    ################################################################################
    # Initialize default command subparser
    ################################################################################
    defaultcmdparser = argparse.ArgumentParser(add_help=False)

    # Option for non-default elasticsearch url
    urlaction = defaultcmdparser.add_argument("-u", "--url", type=str, action=ESAction,
                                              help="elasticsearch url [default localhost:9200]", default="localhost:9200")

    # Subgroup for common query string parameter arguments
    qspargs = defaultcmdparser.add_argument_group("common parameters", description="Optional parameters available to all cat APIs")
    qspargs.add_argument("-c", "--columns", type=str,
                         help="comma-separated list of column names to display (cat API param 'h')")
    # TODO: Figure out what this actually takes and means
    qspargs.add_argument("-f", "--format", type=str,
                         help="a short version of the Accept header (cat API param 'format')")
    qspargs.add_argument("-i", "--information", action="store_true",
                         help="return help information (cat API param 'help')")
    qspargs.add_argument("-s", "--sort", type=str,
                         help="comma-separated list of column names or aliases to sort by (cat API param 's')")
    qspargs.add_argument("-v", "--verbose", action="store_true",
                         help="turn on cat API verbose output (cat API param 'v')")

    ################################################################################
    # Command subparsers: parents=[defaultcmdparser]
    ################################################################################
    aliases_parser = subparsers.add_parser("aliases", parents=[defaultcmdparser])
    aliases_parser.set_defaults(cmd=aliases)
    aliases_parser.add_argument("name", type=str, nargs="?",
                                help="set aliases to display (comma-separated single string)")
    aliases_parser.add_argument("-l", action=LocalOption)
    aliases_parser.add_argument("-t", action=MasterTimeoutOption)

    allocation_parser = subparsers.add_parser("allocation", parents=[defaultcmdparser])
    allocation_parser.set_defaults(cmd=allocation)
    allocation_parser.add_argument("node_id", type=str, nargs="?",
                                   help="set node IDs or names to display (comma-separated single string)")
    allocation_parser.add_argument("-b", action=BytesOption)
    allocation_parser.add_argument("-l", action=LocalOption)
    allocation_parser.add_argument("-t", action=MasterTimeoutOption)

    count_parser = subparsers.add_parser("count", parents=[defaultcmdparser])
    count_parser.set_defaults(cmd=count)
    count_parser.add_argument("index", type=str, nargs="?",
                              help="set indices to display (comma-separated single string)")
    count_parser.add_argument("-t", action=MasterTimeoutOption)

    fielddata_parser = subparsers.add_parser("fielddata", parents=[defaultcmdparser])
    fielddata_parser.set_defaults(cmd=fielddata)
    fielddata_parser.add_argument("field", type=str, nargs="?",
                                  help="set fields to display (comma-separated single string)")
    fielddata_parser.add_argument("-b", action=BytesOption)
    fielddata_parser.add_argument("-t", action=MasterTimeoutOption)

    health_parser = subparsers.add_parser("health", parents=[defaultcmdparser])
    health_parser.set_defaults(cmd=health)
    health_parser.add_argument("-d", "--notime", action="store_false", help="disable timestamping", dest="ts")
    health_parser.add_argument("-t", action=MasterTimeoutOption)
    
    indices_parser = subparsers.add_parser("indices", parents=[defaultcmdparser])
    indices_parser.set_defaults(cmd=indices)
    indices_parser.add_argument("index", type=str, nargs="?",
                                help="set indices to display (comma-separated single string)")
    indices_parser.add_argument("-b", action=BytesOption)
    indices_parser.add_argument("-e", action=HealthOption)
    indices_parser.add_argument("-l", action=LocalOption)
    indices_parser.add_argument("-p", action=PrimaryOption)
    indices_parser.add_argument("-t", action=MasterTimeoutOption)

    master_parser = subparsers.add_parser("master", parents=[defaultcmdparser])
    master_parser.set_defaults(cmd=master)
    master_parser.add_argument("-l", action=LocalOption)
    master_parser.add_argument("-t", action=MasterTimeoutOption)

    nodeattrs_parser = subparsers.add_parser("nodeattrs", parents=[defaultcmdparser])
    nodeattrs_parser.set_defaults(cmd=nodeattrs)
    nodeattrs_parser.add_argument("-l", action=LocalOption)
    nodeattrs_parser.add_argument("-t", action=MasterTimeoutOption)

    nodes_parser = subparsers.add_parser("nodes", parents=[defaultcmdparser])
    nodes_parser.set_defaults(cmd=nodes)
    nodes_parser.add_argument("-d", "--full_id", action="store_true", help="return full node ID instead of shortened version")
    nodes_parser.add_argument("-l", action=LocalOption)
    nodes_parser.add_argument("-t", action=MasterTimeoutOption)

    pending_tasks_parser = subparsers.add_parser("pending_tasks", parents=[defaultcmdparser])
    pending_tasks_parser.set_defaults(cmd=pending_tasks)
    pending_tasks_parser.add_argument("-l", action=LocalOption)
    pending_tasks_parser.add_argument("-t", action=MasterTimeoutOption)

    plugins_parser = subparsers.add_parser("plugins", parents=[defaultcmdparser])
    plugins_parser.set_defaults(cmd=plugins)
    plugins_parser.add_argument("-l", action=LocalOption)
    plugins_parser.add_argument("-t", action=MasterTimeoutOption)

    recovery_parser = subparsers.add_parser("recovery", parents=[defaultcmdparser])
    recovery_parser.set_defaults(cmd=recovery)
    recovery_parser.add_argument("index", type=str, nargs="?",
                                 help="set indices to display (comma-separated single string)")
    recovery_parser.add_argument("-b", action=BytesOption)
    recovery_parser.add_argument("-t", action=MasterTimeoutOption)

    repositories_parser = subparsers.add_parser("repositories", parents=[defaultcmdparser])
    repositories_parser.set_defaults(cmd=repositories)
    repositories_parser.add_argument("-l", action=LocalOption)
    repositories_parser.add_argument("-t", action=MasterTimeoutOption)

    segments_parser = subparsers.add_parser("segments", parents=[defaultcmdparser])
    segments_parser.set_defaults(cmd=segments)
    segments_parser.add_argument("index", type=str, nargs="?",
                                 help="set indices to display (comma-separated single string)")
    segments_parser.add_argument("-b", action=BytesOption)

    shards_parser = subparsers.add_parser("shards", parents=[defaultcmdparser])
    shards_parser.set_defaults(cmd=shards)
    shards_parser.add_argument("index", type=str, nargs="?",
                               help="set indices to display (comma-separated single string)")
    shards_parser.add_argument("-b", action=BytesOption)
    shards_parser.add_argument("-l", action=LocalOption)
    shards_parser.add_argument("-t", action=MasterTimeoutOption)

    snapshots_parser = subparsers.add_parser("snapshots", parents=[defaultcmdparser])
    snapshots_parser.set_defaults(cmd=snapshots)
    snapshots_parser.add_argument("repository", type=str,
                                  help="set repository to display")
    snapshots_parser.add_argument("-g", "--ignore_unavailable", action="store_true", help="ignore unavailable snapshots")
    snapshots_parser.add_argument("-t", action=MasterTimeoutOption)

    templates_parser = subparsers.add_parser("templates", parents=[defaultcmdparser])
    templates_parser.set_defaults(cmd=templates)
    templates_parser.add_argument("name", type=str, nargs="?",
                                  help="set templates to display (comma-separated single string)")
    templates_parser.add_argument("-l", action=LocalOption)
    templates_parser.add_argument("-t", action=MasterTimeoutOption)

    thread_pool_parser = subparsers.add_parser("thread_pool", parents=[defaultcmdparser])
    thread_pool_parser.set_defaults(cmd=thread_pool)
    thread_pool_parser.add_argument("pattern", type=str, nargs="?",
                                    help="set thread pool patterns to match (comma-separated single string)")
    thread_pool_parser.add_argument("-l", action=LocalOption)
    thread_pool_parser.add_argument("-t", action=MasterTimeoutOption)
    thread_pool_parser.add_argument("-z", action=SizeOption)

    ################################################################################
    # Parse Arguments
    ################################################################################
    # TODO: This is terrible. If -u is supplied, I create the connection twice (once for default, once for change). I hate it.
    # Cause ESAction to trigger on default...
    defaultnamespace = argparse.Namespace()
    urlaction(parser, defaultnamespace, urlaction.default)
    args = parser.parse_args(namespace=defaultnamespace)
    print(catgeneric(args))
