// PgBolt dashboard: локальное время, polling только на видимой вкладке, уведомления, «выбрать все». Без eval и inline-скриптов.
(function () {
    'use strict';
    var relative = new Intl.RelativeTimeFormat('ru', { numeric: 'auto' });
    var units = [['day', 86400], ['hour', 3600], ['minute', 60], ['second', 1]];

    function ago(date) {
        var seconds = (date.getTime() - Date.now()) / 1000;
        for (var i = 0; i < units.length; i++)
            if (Math.abs(seconds) >= units[i][1] || units[i][1] === 1)
                return relative.format(Math.round(seconds / units[i][1]), units[i][0]);
    }

    htmx.onLoad(function (root) {
        root.querySelectorAll('time[datetime]').forEach(function (el) {
            var dt = el.getAttribute('datetime');
            var date = new Date(dt);
            if (isNaN(date.getTime())) return;
            el.textContent = date.toLocaleString();
            el.title = dt + ' (' + ago(date) + ')';
        });
    });

    // Polling пропускается на скрытой вкладке и пока в обновляемом блоке вводят значение: замена строк стёрла бы ввод.
    document.addEventListener('htmx:beforeRequest', function (e) {
        var elt = e.detail.elt, active = document.activeElement;
        if (elt.hasAttribute('data-pgbolt-poll') && (document.hidden || (active && active.tagName === 'INPUT' && elt.contains(active))))
            e.preventDefault();
    });

    document.addEventListener('visibilitychange', function () {
        if (!document.hidden)
            document.querySelectorAll('[data-pgbolt-poll]').forEach(function (el) { htmx.trigger(el, 'pgbolt:refresh'); });
    });

    var notifyTimer;
    document.addEventListener('pgbolt:notify', function (e) {
        var box = document.getElementById('pgbolt-notify');
        if (!box) return;
        box.textContent = e.detail.value;
        clearTimeout(notifyTimer);
        notifyTimer = setTimeout(function () { box.textContent = ''; }, 5000);
    });

    document.addEventListener('change', function (e) {
        var all = e.target.closest && e.target.closest('[data-pgbolt-select-all]');
        if (!all) return;
        var form = all.closest('form') || document;
        form.querySelectorAll('input[type=checkbox][name]').forEach(function (box) { box.checked = all.checked; });
    });
})();
