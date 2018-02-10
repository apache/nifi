# NiFi CLI

This tool offers a CLI focused on interacting with NiFi and NiFi Registry in order to automate tasks, such 
as deploying flows from a NIFi Registy to a NiFi instance.

## Usage

The CLI toolkit can be executed in standalone mode to execute a single command, or interactive mode to enter 
an interactive shell.

To execute a single command:

    ./bin/cli.sh <command> <args>
    
To launch the interactive shell:

    ./bin/cli.sh 
    
## Property/Argument Handling

Most commands will require specifying a baseUrl for the NiFi or NiFi registry instance.

An example command to list the buckets in a NiFi Registry instance would be the following:

    ./bin/cli.sh nifi-reg list-buckets -u http://localhost:18080 

In order to avoid specifying the URL (and possibly other optional arguments for TLS) on every command, 
you can define a properties file containing the reptitive arguments.

An example properties file for a local NiFi Registry instance would look like the following:

    baseUrl=https://localhost:18443
    keystore=
    keystoreType=
    keystorePasswd=
    keyPasswd=
    truststore=
    truststoreType=
    truststorePasswd=

This properties file can then be used on a command by specifying -p <path-to-props-file> :

    ./bin/cli.sh nifi-reg list-buckets -p /path/to/local-nifi-registry.properties
    
You could then maintain a properties file for each environment you plan to interact with, such as dev, qa, prod.

In addition to specifying, a properties file on each command, you can setup a default properties file to 
be used in the event that no properties file is specified.

The default properties file is specified using the session concept, which persists to the users home 
directory in a file called *.nifi-cli.config*.

An example of setting the default property files would be following:

    ./bin/cli.sh session set nifi.props /path/to/local-nifi.properties
    ./bin/cli.sh session set nifi.reg.props /path/to/local-nifi-registry.properties

This will write the above properties into the .nifi-cli.config in the user's home directory and will 
allow commands to be executed without specifying a URL or properties file:

    ./bin/cli.sh nifi-reg list-buckets
    
The above command will now use the baseUrl from *local-nifi-registry.properties*.

The order of resolving an argument is the following:

* A direct argument overrides anything in a properties file or session
* A properties file argument (-p) overrides the session
* The session is used when nothing else is specified

## Interactive Usage

In interactive mode the tab key can be used to perform auto-completion.

For example, typing tab at an empty prompt should display possible commands for the first argument:

    #>
    exit       help       nifi       nifi-reg   session
    
Typing "nifi " and then a tab will show the sub-commands for NiFi:

    #> nifi
    create-reg-client   current-user        get-root-id         list-reg-clients    pg-get-vars         pg-import       update-reg-client
    
Arguments that represent a path to a file, such as -p or when setting a properties file in the session, 
will auto-complete the path being typed:
 
    #> session set nifi.props /tmp/
    dir1/   dir2/   dir3/
    
## Output

All commands (except export-flow-version) support the ability to specify an <code>--outputType</code> argument, 
or <code>-ot</code> for short.

Currently the output type may be <code>simple</code> or <code>json</code>.

The default output type in interactive mode is <code>simple</code>, 
and the default output type in standalone mode is <code>json</code>.

Example of simple output for list-buckets:

    #> registry list-buckets -ot simple
    
    My Bucket - 3c7b7467-0012-4d8f-a918-6aa42b6b9d39

Example of json output for list-buckets:

    #> registry list-buckets -ot json
    [ {
      "identifier" : "3c7b7467-0012-4d8f-a918-6aa42b6b9d39",
      "name" : "My Bucket",
      "createdTimestamp" : 1516718733854,
      "permissions" : {
        "canRead" : true,
        "canWrite" : true,
        "canDelete" : true
      },
      "link" : {
        "params" : {
          "rel" : "self"
        },
        "href" : "buckets/3c7b7467-0012-4d8f-a918-6aa42b6b9d39"
      }
    } ]