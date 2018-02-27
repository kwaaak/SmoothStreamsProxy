.. :changelog:

Release History
===============

1.1.0 (2018-27-02)
------------------

* Added validations when parsing the configuration file along with error messages
* Added a timer that will automatically retrieve a new authorization hash
    * The timer will trigger 45 seconds before the authorization hash is set to expire
    * If a new authorization hash is retrieved by a client request (As a result of a request to http://<hostname>:<port>/playlist.m3u8?channel_number=XX) then the current timer is cancelled and a new timer is initiated
* Added watchdog functionality that will monitor the configuration file for modifications
* Added functionality to obfuscate/encrypt the password in the configuration file following the first run
* Lots of refactoring and various bug fixes

1.0.0 (2018-24-02)
------------------

* First public release
