[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Join CDAP community](https://cdap-users.herokuapp.com/badge.svg?t=salesforce-plugins)](https://cdap-users.herokuapp.com?t=1)

# Overview

Following plugins are available in this repository.

  * Salesforce Batch Source
  * Salesforce Streaming Source

# Integration tests

By default all integration tests will be skipped, since Salesforce credentials are needed.

Instructions to run the tests:
 1. Create/use existing Salesforce account
 2. Create connected application within the account to get consumerKey and consumerSecret
 3. Run the tests using the command below:

```
mvn clean test -Dsalesforce.test.consumerKey= -Dsalesforce.test.consumerSecret= -Dsalesforce.test.username= -Dsalesforce.test.password=
```

# Contact

## Mailing Lists

CDAP User Group and Development Discussions:

* [cdap-user@googlegroups.com](https://groups.google.com/d/forum/cdap-user)

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from
users, release announcements, and any other discussions that we think will be helpful
to the users.

## Slack Team

CDAP Users on Slack: [cdap-users team](https://cdap-users.herokuapp.com)


# License and Trademarks

Copyright © 2019 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
