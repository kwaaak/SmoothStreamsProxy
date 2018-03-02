.. :changelog:

Release History
===============

1.2.2 (02-03-2018)
------------------
* Fixed a bug where the channel_number and client_uuid were not being added to the chunks.m3u8 link after hijacking the Nimble session
* Fixed a bug where the watchdog path being monitored for configuration file modifications was always set to the script's current working directory. Now the watchdog path being monitored is the full path to the parent folder of the configuration file
* Significant refactoring and various other minor bug fixes

1.2.1 (01-03-2018)
------------------

* Code refactoring and various bug fixes

1.2.0 (28-02-2018)
------------------

* Added nimble session "hijacking"
    * The chunks.m3u8 link returned by SmoothStreams contains 2 parameters (nimblesessionid & wmsAuthSign)
    * wmsAuthSign is the authorization hash
    * The chunks.m3u8 link is only updated if a user switches to a different channel. As long as the same channel is
    being watched, the same chunks.m3u8 link is being used
    * As a result if the authorization hash expires while a channel is being watched the stream will stop until the user
    switches channels to retrieve a new authorization hash
    * The functionality added is to prevent this from happening by manipulating the values of the 2 parameters
    (nimblesessionid & wmsAuthSign) to valid values
* Code refactoring and various bug fixes

1.1.0 (27-02-2018)
------------------

* Added validations when parsing the configuration file along with error messages
* Added a timer that will automatically retrieve a new authorization hash
    * The timer will trigger 45 seconds before the authorization hash is set to expire
    * If a new authorization hash is retrieved by a client request (As a result of a request to http://<hostname>:<port>/playlist.m3u8?channel_number=XX) then the current timer is cancelled and a new timer is initiated
* Added watchdog functionality that will monitor the configuration file for modifications
* Added functionality to obfuscate/encrypt the password in the configuration file following the first run
* Lots of refactoring and various bug fixes

1.0.0 (24-02-2018)
------------------

* First public release
