# Cloud Haskell over AMQP (RabbitMQ)

[![Hackage package][Hackage Version Image]][Hackage package]
[![Build Status][Build Status Image]][Build Status]

[Hackage Version Image]: http://img.shields.io/hackage/v/network-transport-amqp.svg
[Hackage package]: http://hackage.haskell.org/package/network-transport-amqp
[Build Status Image]: https://travis-ci.org/adinapoli/network-transport-amqp.svg?branch=master
[Build Status]: http://travis-ci.org/adinapoli/network-transport-amqp

[network-transport][network-transport] backend based on the
[RabbitMQ][zeromq] distributed messaging platform. 


[network-transport]: http://hackage.haskell.org/package/network-transport
[rabbitmq]: https://www.rabbitmq.com

Status
------

- [X] Runs against [network-transport-tests]
- [ ] Runs against [distributed-process-tests]
      - [ ] Runs against "ch-core" (threw exception - 1 hangs, rest passes)
      - [ ] Runs against "ch-closure" (1 fails, 1 hangs, rest passes)
      - [X] Runs against "ch-stats"
- [ ] Supports multicast

[network-transport-tests]: http://hackage.haskell.org/package/network-transport-tests
[distributed-process-tests]: https://github.com/haskell-distributed/distributed-process-tests


Credits
-------

Most of the credits go to [TweagIO](http://tweag.io), which released a
transport layer called [network-transport-zeromq][zmq], which was heavily
borrowed for the creation of this project. Without their initial effort, none
of this would have been possible.

![IRIS](http://www.iconnect-online.com/img/iris-logo.png)

Thanks to [IRIS Connect][iconnect] for funding this project, allowing me to
work on it.

network-transport-amqp is maintained and funded by [IRIS Connect][iconnect].

[zmq]: https://github.com/tweag/network-transport-zeromq
[iconnect]: http://irisconnect.co.uk

License
-------

Check the `TWEAG_LICENSE` license file for the license this project depends
upon.

Contributions
-------------
This library scratches my own itches, but please fork away!
Pull requests are encouraged.
