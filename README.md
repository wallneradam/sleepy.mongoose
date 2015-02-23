DISCLAIMER
==========
Please note: all tools/ scripts in this repo are released for use "AS IS" without any warranties of any kind, including, but not limited to their installation, use, or performance. We disclaim any and all warranties, either express or implied, including but not limited to any warranty of noninfringement, merchantability, and/ or fitness for a particular purpose. We do not warrant that the technology will meet your requirements, that the operation thereof will be uninterrupted or error-free, or that any errors will be corrected.
Any use of these scripts and tools is at your own risk. There is no guarantee that they have been through thorough testing in a comparable environment and we are not responsible for any damage or data loss incurred with their use.
You are responsible for reviewing and testing any scripts you run thoroughly before use in any non-testing environment.

See [the original wiki](https://github.com/10gen-labs/sleepy.mongoose/wiki) for documentation.

This is a modified, optimised version of the original Sleepy Mongoose.
New features:

* You can mix POST and GET requests, it is the same parameter in different transfer method. Post has more precendency.

* You can specify listen host and port

* Disconnect method (_disconnect)

* Insert or update method (_insert_or_update), which is a shorcut to upsert update

* Shell script to start/stop or run in debug mode (Unix/Linux)

