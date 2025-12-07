/****************************************************************************
** Meta object code from reading C++ file 'weather.h'
**
** Created by: The Qt Meta Object Compiler version 67 (Qt 5.12.8)
**
** WARNING! All changes made in this file will be lost!
*****************************************************************************/

#include "weather.h"
#include <QtCore/qbytearray.h>
#include <QtCore/qmetatype.h>
#if !defined(Q_MOC_OUTPUT_REVISION)
#error "The header file 'weather.h' doesn't include <QObject>."
#elif Q_MOC_OUTPUT_REVISION != 67
#error "This file was generated using the moc from 5.12.8. It"
#error "cannot be used with the include files from this version of Qt."
#error "(The moc has changed too much.)"
#endif

QT_BEGIN_MOC_NAMESPACE
QT_WARNING_PUSH
QT_WARNING_DISABLE_DEPRECATED
struct qt_meta_stringdata_Weather_t {
    QByteArrayData data[17];
    char stringdata0[214];
};
#define QT_MOC_LITERAL(idx, ofs, len) \
    Q_STATIC_BYTE_ARRAY_DATA_HEADER_INITIALIZER_WITH_OFFSET(len, \
    qptrdiff(offsetof(qt_meta_stringdata_Weather_t, stringdata0) + ofs \
        - idx * sizeof(QByteArrayData)) \
    )
static const qt_meta_stringdata_Weather_t qt_meta_stringdata_Weather = {
    {
QT_MOC_LITERAL(0, 0, 7), // "Weather"
QT_MOC_LITERAL(1, 8, 19), // "weatherDataReceived"
QT_MOC_LITERAL(2, 28, 0), // ""
QT_MOC_LITERAL(3, 29, 8), // "cityName"
QT_MOC_LITERAL(4, 38, 11), // "temperature"
QT_MOC_LITERAL(5, 50, 18), // "weatherDescription"
QT_MOC_LITERAL(6, 69, 8), // "iconCode"
QT_MOC_LITERAL(7, 78, 20), // "forecastDataReceived"
QT_MOC_LITERAL(8, 99, 12), // "forecastData"
QT_MOC_LITERAL(9, 112, 13), // "errorOccurred"
QT_MOC_LITERAL(10, 126, 11), // "errorString"
QT_MOC_LITERAL(11, 138, 14), // "onNetworkReply"
QT_MOC_LITERAL(12, 153, 14), // "QNetworkReply*"
QT_MOC_LITERAL(13, 168, 5), // "reply"
QT_MOC_LITERAL(14, 174, 16), // "fetchWeatherData"
QT_MOC_LITERAL(15, 191, 4), // "city"
QT_MOC_LITERAL(16, 196, 17) // "fetch5daysWeather"

    },
    "Weather\0weatherDataReceived\0\0cityName\0"
    "temperature\0weatherDescription\0iconCode\0"
    "forecastDataReceived\0forecastData\0"
    "errorOccurred\0errorString\0onNetworkReply\0"
    "QNetworkReply*\0reply\0fetchWeatherData\0"
    "city\0fetch5daysWeather"
};
#undef QT_MOC_LITERAL

static const uint qt_meta_data_Weather[] = {

 // content:
       8,       // revision
       0,       // classname
       0,    0, // classinfo
       6,   14, // methods
       0,    0, // properties
       0,    0, // enums/sets
       0,    0, // constructors
       0,       // flags
       3,       // signalCount

 // signals: name, argc, parameters, tag, flags
       1,    4,   44,    2, 0x06 /* Public */,
       7,    2,   53,    2, 0x06 /* Public */,
       9,    1,   58,    2, 0x06 /* Public */,

 // slots: name, argc, parameters, tag, flags
      11,    1,   61,    2, 0x08 /* Private */,

 // methods: name, argc, parameters, tag, flags
      14,    1,   64,    2, 0x02 /* Public */,
      16,    1,   67,    2, 0x02 /* Public */,

 // signals: parameters
    QMetaType::Void, QMetaType::QString, QMetaType::Double, QMetaType::QString, QMetaType::QString,    3,    4,    5,    6,
    QMetaType::Void, QMetaType::QString, QMetaType::QVariantList,    3,    8,
    QMetaType::Void, QMetaType::QString,   10,

 // slots: parameters
    QMetaType::Void, 0x80000000 | 12,   13,

 // methods: parameters
    QMetaType::Void, QMetaType::QString,   15,
    QMetaType::Void, QMetaType::QString,   15,

       0        // eod
};

void Weather::qt_static_metacall(QObject *_o, QMetaObject::Call _c, int _id, void **_a)
{
    if (_c == QMetaObject::InvokeMetaMethod) {
        auto *_t = static_cast<Weather *>(_o);
        Q_UNUSED(_t)
        switch (_id) {
        case 0: _t->weatherDataReceived((*reinterpret_cast< const QString(*)>(_a[1])),(*reinterpret_cast< double(*)>(_a[2])),(*reinterpret_cast< const QString(*)>(_a[3])),(*reinterpret_cast< const QString(*)>(_a[4]))); break;
        case 1: _t->forecastDataReceived((*reinterpret_cast< const QString(*)>(_a[1])),(*reinterpret_cast< const QVariantList(*)>(_a[2]))); break;
        case 2: _t->errorOccurred((*reinterpret_cast< const QString(*)>(_a[1]))); break;
        case 3: _t->onNetworkReply((*reinterpret_cast< QNetworkReply*(*)>(_a[1]))); break;
        case 4: _t->fetchWeatherData((*reinterpret_cast< const QString(*)>(_a[1]))); break;
        case 5: _t->fetch5daysWeather((*reinterpret_cast< const QString(*)>(_a[1]))); break;
        default: ;
        }
    } else if (_c == QMetaObject::RegisterMethodArgumentMetaType) {
        switch (_id) {
        default: *reinterpret_cast<int*>(_a[0]) = -1; break;
        case 3:
            switch (*reinterpret_cast<int*>(_a[1])) {
            default: *reinterpret_cast<int*>(_a[0]) = -1; break;
            case 0:
                *reinterpret_cast<int*>(_a[0]) = qRegisterMetaType< QNetworkReply* >(); break;
            }
            break;
        }
    } else if (_c == QMetaObject::IndexOfMethod) {
        int *result = reinterpret_cast<int *>(_a[0]);
        {
            using _t = void (Weather::*)(const QString & , double , const QString & , const QString & );
            if (*reinterpret_cast<_t *>(_a[1]) == static_cast<_t>(&Weather::weatherDataReceived)) {
                *result = 0;
                return;
            }
        }
        {
            using _t = void (Weather::*)(const QString & , const QVariantList & );
            if (*reinterpret_cast<_t *>(_a[1]) == static_cast<_t>(&Weather::forecastDataReceived)) {
                *result = 1;
                return;
            }
        }
        {
            using _t = void (Weather::*)(const QString & );
            if (*reinterpret_cast<_t *>(_a[1]) == static_cast<_t>(&Weather::errorOccurred)) {
                *result = 2;
                return;
            }
        }
    }
}

QT_INIT_METAOBJECT const QMetaObject Weather::staticMetaObject = { {
    &QObject::staticMetaObject,
    qt_meta_stringdata_Weather.data,
    qt_meta_data_Weather,
    qt_static_metacall,
    nullptr,
    nullptr
} };


const QMetaObject *Weather::metaObject() const
{
    return QObject::d_ptr->metaObject ? QObject::d_ptr->dynamicMetaObject() : &staticMetaObject;
}

void *Weather::qt_metacast(const char *_clname)
{
    if (!_clname) return nullptr;
    if (!strcmp(_clname, qt_meta_stringdata_Weather.stringdata0))
        return static_cast<void*>(this);
    return QObject::qt_metacast(_clname);
}

int Weather::qt_metacall(QMetaObject::Call _c, int _id, void **_a)
{
    _id = QObject::qt_metacall(_c, _id, _a);
    if (_id < 0)
        return _id;
    if (_c == QMetaObject::InvokeMetaMethod) {
        if (_id < 6)
            qt_static_metacall(this, _c, _id, _a);
        _id -= 6;
    } else if (_c == QMetaObject::RegisterMethodArgumentMetaType) {
        if (_id < 6)
            qt_static_metacall(this, _c, _id, _a);
        _id -= 6;
    }
    return _id;
}

// SIGNAL 0
void Weather::weatherDataReceived(const QString & _t1, double _t2, const QString & _t3, const QString & _t4)
{
    void *_a[] = { nullptr, const_cast<void*>(reinterpret_cast<const void*>(&_t1)), const_cast<void*>(reinterpret_cast<const void*>(&_t2)), const_cast<void*>(reinterpret_cast<const void*>(&_t3)), const_cast<void*>(reinterpret_cast<const void*>(&_t4)) };
    QMetaObject::activate(this, &staticMetaObject, 0, _a);
}

// SIGNAL 1
void Weather::forecastDataReceived(const QString & _t1, const QVariantList & _t2)
{
    void *_a[] = { nullptr, const_cast<void*>(reinterpret_cast<const void*>(&_t1)), const_cast<void*>(reinterpret_cast<const void*>(&_t2)) };
    QMetaObject::activate(this, &staticMetaObject, 1, _a);
}

// SIGNAL 2
void Weather::errorOccurred(const QString & _t1)
{
    void *_a[] = { nullptr, const_cast<void*>(reinterpret_cast<const void*>(&_t1)) };
    QMetaObject::activate(this, &staticMetaObject, 2, _a);
}
QT_WARNING_POP
QT_END_MOC_NAMESPACE
