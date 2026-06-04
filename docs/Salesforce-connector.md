# Salesforce Connection

Description
-----------
Use this connection to access data in Salesforce.

Properties
----------
**Name:** Name of the connection. Connection names must be unique in a namespace.

**Description:** Description of the connection.

**Grant Type:** Grant type to use for OAuth authentication. Supported values are 'password' and
'client_credentials'. When set to 'client_credentials', only Consumer Key, Consumer Secret, and Login URL
are required. Username, Password, and Security Token are not needed. Defaults to 'password' if not specified.

**Username:** Salesforce username. Required for 'password' grant type.

**Password:** Salesforce password. Required for 'password' grant type.

**Security Token:** Salesforce security token. If the password does not contain the security token, the plugin
will append the token before authenticating with Salesforce. Only applicable for 'password' grant type.

**Consumer Key:** Application Consumer Key. This is also known as the OAuth client ID.
A Salesforce connected application must be created in order to get a consumer key.

**Consumer Secret:** Application Consumer Secret. This is also known as the OAuth client secret.
A Salesforce connected application must be created in order to get a client secret.

**Login URL:** Salesforce OAuth2 login URL. For the 'password' grant type, the default generic URL
`https://login.salesforce.com/services/oauth2/token` can be used. For the 'client_credentials' grant type,
you must provide your Salesforce instance-specific URL, for example
`https://<your-instance>.my.salesforce.com/services/oauth2/token`.

**Connect Timeout:** Maximum time in milliseconds to wait for connection initialization before it times out.

**Read Timeout:** Maximum time in seconds to wait for reading data from the server before it times out.

**Proxy URL:** Proxy URL. Must contain a protocol, address and port.

Path of the connection
----------------------
To browse, get a sample from, or get the specification for this connection. (Not supported in the Salesforce Streaming
source and Salesforce Multi Object batch source.).  
/{object} This path indicates a Salesforce object. An object is the only one that can be sampled.