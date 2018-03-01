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

    ./bin/cli.sh registry list-buckets -u http://localhost:18080 

In order to avoid specifying the URL (and possibly other optional arguments for TLS) on every command, 
you can define a properties file containing the repetitive arguments.

An example properties file for a local NiFi Registry instance would look like the following:

    baseUrl=http://localhost:18080
    keystore=
    keystoreType=
    keystorePasswd=
    keyPasswd=
    truststore=
    truststoreType=
    truststorePasswd=
    proxiedEntity=

This properties file can then be used on a command by specifying -p <path-to-props-file> :

    ./bin/cli.sh registry list-buckets -p /path/to/local-nifi-registry.properties
    
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

    ./bin/cli.sh registry list-buckets
    
The above command will now use the baseUrl from *local-nifi-registry.properties*.

The order of resolving an argument is the following:

* A direct argument overrides anything in a properties file or session
* A properties file argument (-p) overrides the session
* The session is used when nothing else is specified

## Security Configuration

If NiFi and NiFi Registry are secured, then commands executed from the CLI will need to make a TLS connection and 
authenticate as a user with permissions to perform the desired action. 

Currently the CLI supports authenticating with a client certificate and an optional proxied-entity. A common scenario 
would be running the CLI from one of the nodes where NiFi or NiFi Registry is installed, which allows the CLI to use 
the same key store and trust store as the NiFi/NiFi Registry instance.

The security configuration can be specified per-command, or in one of the properties files described in the previous section.

The examples below are for NiFi Registry, but the same concept applies for NiFi commands. 

### Example - Secure NiFi Registry without Proxied-Entity

Assuming we have a keystore containing the certificate for *'CN=user1, OU=NIFI'*, an example properties file would 
be the following:

    baseUrl=https://localhost:18443
    keystore=/path/to/keystore.jks
    keystoreType=JKS
    keystorePasswd=changeme
    keyPasswd=changeme
    truststore=/path/to/truststore.jks
    truststoreType=JKS
    truststorePasswd=changeme

In this example, commands will be executed as *'CN=user1, OU=NIFI'*. This user would need to be a user in NiFi Registry, 
and commands accessing buckets would be restricted to buckets this user has access to.

### Example - Secure NiFi Registry with Proxied-Entity

Assuming we have access to the keystore of NiFi Registry itself, and that NiFi Registry is also configured to allow 
Kerberos or LDAP authentication, an example properties file would be the following:

    baseUrl=https://localhost:18443
    keystore=/path/to/keystore.jks
    keystoreType=JKS
    keystorePasswd=changeme
    keyPasswd=changeme
    truststore=/path/to/truststore.jks
    truststoreType=JKS
    truststorePasswd=changeme
    proxiedEntity=user1@NIFI.COM

In this example, the certificate in keystore.jks would be for the NiFi Registry server, for example *'CN=localhost, OU=NIFI'*. 
This identity would need to be defined as a user in NiFi Registry and given permissions to 'Proxy'.

*'CN=localhost, OU=NIFI'* would be proxying commands to be executed as *'user1@NIFI.COM'*.

## Interactive Usage

In interactive mode the tab key can be used to perform auto-completion.

For example, typing tab at an empty prompt should display possible commands for the first argument:

    #>
    exit       help       nifi       registry   session
    
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
    
## Back Referencing

When using the interactive CLI, a common scenario will be using an id from a previous 
result as the input to the next command. Back-referencing provides a shortcut for 
referencing a result from the previous command via a positional reference.

NOTE: Not every command produces back-references. To determine if a command 
supports back-referencing, check the usage.

      #> registry list-buckets help
      
      Lists the buckets that the current user has access to.
      
      PRODUCES BACK-REFERENCES

A common scenario for utilizing back-references would be the following:

1) User starts by exploring the available buckets in a registry instance

        #> registry list-buckets
        
        #   Name           Id                                     Description
        -   ------------   ------------------------------------   -----------
        1   My Bucket      3c7b7467-0012-4d8f-a918-6aa42b6b9d39   (empty)
        2   Other Bucket   175fb557-43a2-4abb-871f-81a354f47bc2   (empty)

2) User then views the flows in one of the buckets using a back-reference to the bucket id from the previous result in position 1

        #> registry list-flows -b &1
        
        Using a positional back-reference for 'My Bucket'
        
        #   Name      Id                                     Description
        -   -------   ------------------------------------   ----------------
        1   My Flow   06acb207-d2f1-447f-85ed-9b8672fe6d30   This is my flow.

3) User then views the version of the flow using a back-reference to the flow id from the previous result in position 1

        #> registry list-flow-versions -f &1
        
        Using a positional back-reference for 'My Flow'
        
        Ver   Date                         Author                     Message
        ---   --------------------------   ------------------------   -------------------------------------
        1     Tue, Jan 23 2018 09:48 EST   anonymous                  This is the first version of my flow.

4) User deploys version 1 of the flow using back-references to the bucket and flow id from step 2

        #> nifi pg-import -b &1 -f &1 -fv 1
        
        Using a positional back-reference for 'My Bucket'
        
        Using a positional back-reference for 'My Flow'
        
        9bd157d4-0161-1000-b946-c1f9b1832efd    
    
The reason step 4 was able to reference the results from step 2, is because the list-flow-versions 
command in step 3 does not produce back-references, so the results from step 2 are still available.
    
## Adding Commands

To add a NiFi command, create a new class that extends AbstractNiFiCommand:

    public class MyCommand extends AbstractNiFiCommand {
    
      public MyCommand() {
          super("my-command");
      }
      
      @Override
      protected void doExecute(NiFiClient client, Properties properties) 
              throws NiFiClientException, IOException, MissingOptionException, CommandException {
          // TODO implement        
      }
  
      @Override
      public String getDescription() {
          return "This is my new command";
      }
    }
    
Add the new command to NiFiCommandGroup:

    commands.add(new MyCommand());

To add a NiFi Registry command, perform the same steps, but extend from 
AbstractNiFiRegistryCommand, and add the command to NiFiRegistryCommandGroup.
