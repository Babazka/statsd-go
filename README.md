STATSD-MONITOR
==============

<!--
[![Build Status](https://secure.travis-ci.org/Babazka/statsd-monitor.png)](http://travis-ci.org/Babazka/statsd-monitor)
-->

Сбор и агрегация статистических данных по протоколу statsd + хранение данных и рисование графиков с помощью RRD в одном бинарнике.

Основано на [STATSD-GO](https://github.com/jbuchbinder/statsd-go).

Сборка с librrd
---------------

`go build`

Запуск: `./statsd-monitor`

Зависимости:
 * `librrd-dev` для сборки
 * `librrd` для работы

