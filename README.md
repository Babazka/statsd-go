STATSD-MONITOR
==============

<!--
[![Build Status](https://secure.travis-ci.org/Babazka/statsd-monitor.png)](http://travis-ci.org/Babazka/statsd-monitor)
-->

Сбор и агрегация статистических данных по протоколу statsd + хранение данных и рисование графиков с помощью RRD в одном бинарнике.

Основано на [STATSD-GO](https://github.com/jbuchbinder/).

Сборка с librrd
---------------

`go build -tags rrd`

Запуск: `./statsd-monitor --rrd`

Зависимости:
 * `librrd-dev` для сборки
 * `librrd` для работы

Сборка с поддержкой whisper-файлов
----------------------------------

`go build`

Запуск: `./statsd-monitor --rrd`

Соответственно, библиотека `librrd` не нужна ни при сборке, ни при выполнении.


TODO
----

 * <del>подключить https://github.com/ziutek/rrd</del>
 * <del>подключить https://github.com/robyoung/go-whisper</del>
 * нарисовать веб-интерфейс для просмотра графиков

