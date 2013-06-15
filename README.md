STATSD-MONITOR
==============

<!--
[![Build Status](https://secure.travis-ci.org/Babazka/statsd-monitor.png)](http://travis-ci.org/Babazka/statsd-monitor)
-->

Сбор и агрегация статистических данных по протоколу statsd + хранение данных и рисование графиков с помощью RRD в одном бинарнике.

Основано на [STATSD-GO](https://github.com/jbuchbinder/).

Зависимости:
 * `librrd-dev` для сборки
 * `librrd` для работы

TODO:

 * <del>подключить https://github.com/ziutek/rrd</del>
 * нарисовать веб-интерфейс для просмотра графиков
 * добавить отправку в upstream для создания деревьев из statsd-monitor'ов

