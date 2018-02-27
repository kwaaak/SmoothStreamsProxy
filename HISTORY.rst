.. :changelog:

Release History
===============

1.1.0 (2018-27-02)
------------------

* Added validations when parsing the configuration file along with error messages
* Added a timer that will automatically update the authorization hash in case there are no active clients
** The timer will trigger 45 seconds before the authorization hash is set to expire
** If the authorization hash is updated by a client request then the current timer is cancelled and a new timer is initiated
** Practically this timer will only update the authorization hash if explicit update is performed as a result of a client requesting a channel
* Added watchdog functionality that will monitor the configuration file for modifications
* Added functionality to obfuscate/encrypt the password in the configuration file following the first run
* Lots of refactoring and various bug fixes

1.0.0 (2018-24-02)
------------------

* First public release
