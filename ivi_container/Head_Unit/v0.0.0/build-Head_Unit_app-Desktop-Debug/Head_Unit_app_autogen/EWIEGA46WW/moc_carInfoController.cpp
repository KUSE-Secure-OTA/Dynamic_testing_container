/****************************************************************************
** Meta object code from reading C++ file 'carInfoController.h'
**
** Created by: The Qt Meta Object Compiler version 67 (Qt 5.12.8)
**
** WARNING! All changes made in this file will be lost!
*****************************************************************************/

#include "carInfoController.h"
#include <QtCore/qbytearray.h>
#include <QtCore/qmetatype.h>
#if !defined(Q_MOC_OUTPUT_REVISION)
#error "The header file 'carInfoController.h' doesn't include <QObject>."
#elif Q_MOC_OUTPUT_REVISION != 67
#error "This file was generated using the moc from 5.12.8. It"
#error "cannot be used with the include files from this version of Qt."
#error "(The moc has changed too much.)"
#endif

QT_BEGIN_MOC_NAMESPACE
QT_WARNING_PUSH
QT_WARNING_DISABLE_DEPRECATED
struct qt_meta_stringdata_CarInfoController_t {
    QByteArrayData data[24];
    char stringdata0[320];
};
#define QT_MOC_LITERAL(idx, ofs, len) \
    Q_STATIC_BYTE_ARRAY_DATA_HEADER_INITIALIZER_WITH_OFFSET(len, \
    qptrdiff(offsetof(qt_meta_stringdata_CarInfoController_t, stringdata0) + ofs \
        - idx * sizeof(QByteArrayData)) \
    )
static const qt_meta_stringdata_CarInfoController_t qt_meta_stringdata_CarInfoController = {
    {
QT_MOC_LITERAL(0, 0, 17), // "CarInfoController"
QT_MOC_LITERAL(1, 18, 19), // "batteryLevelChanged"
QT_MOC_LITERAL(2, 38, 0), // ""
QT_MOC_LITERAL(3, 39, 16), // "modeColorChanged"
QT_MOC_LITERAL(4, 56, 16), // "directionChanged"
QT_MOC_LITERAL(5, 73, 19), // "batteryValueChanged"
QT_MOC_LITERAL(6, 93, 16), // "distValueChanged"
QT_MOC_LITERAL(7, 110, 15), // "distanceChanged"
QT_MOC_LITERAL(8, 126, 10), // "getBattery"
QT_MOC_LITERAL(9, 137, 22), // "generateRandomDistance"
QT_MOC_LITERAL(10, 160, 15), // "setBatteryLevel"
QT_MOC_LITERAL(11, 176, 5), // "level"
QT_MOC_LITERAL(12, 182, 12), // "setModeColor"
QT_MOC_LITERAL(13, 195, 5), // "color"
QT_MOC_LITERAL(14, 201, 12), // "setDirection"
QT_MOC_LITERAL(15, 214, 9), // "direction"
QT_MOC_LITERAL(16, 224, 11), // "setDistance"
QT_MOC_LITERAL(17, 236, 8), // "distance"
QT_MOC_LITERAL(18, 245, 12), // "batteryLevel"
QT_MOC_LITERAL(19, 258, 9), // "modeColor"
QT_MOC_LITERAL(20, 268, 14), // "blinkDirection"
QT_MOC_LITERAL(21, 283, 12), // "batteryValue"
QT_MOC_LITERAL(22, 296, 12), // "distanceRand"
QT_MOC_LITERAL(23, 309, 10) // "distanceCM"

    },
    "CarInfoController\0batteryLevelChanged\0"
    "\0modeColorChanged\0directionChanged\0"
    "batteryValueChanged\0distValueChanged\0"
    "distanceChanged\0getBattery\0"
    "generateRandomDistance\0setBatteryLevel\0"
    "level\0setModeColor\0color\0setDirection\0"
    "direction\0setDistance\0distance\0"
    "batteryLevel\0modeColor\0blinkDirection\0"
    "batteryValue\0distanceRand\0distanceCM"
};
#undef QT_MOC_LITERAL

static const uint qt_meta_data_CarInfoController[] = {

 // content:
       8,       // revision
       0,       // classname
       0,    0, // classinfo
      12,   14, // methods
       6,   94, // properties
       0,    0, // enums/sets
       0,    0, // constructors
       0,       // flags
       6,       // signalCount

 // signals: name, argc, parameters, tag, flags
       1,    0,   74,    2, 0x06 /* Public */,
       3,    0,   75,    2, 0x06 /* Public */,
       4,    0,   76,    2, 0x06 /* Public */,
       5,    0,   77,    2, 0x06 /* Public */,
       6,    0,   78,    2, 0x06 /* Public */,
       7,    0,   79,    2, 0x06 /* Public */,

 // slots: name, argc, parameters, tag, flags
       8,    0,   80,    2, 0x0a /* Public */,
       9,    0,   81,    2, 0x08 /* Private */,

 // methods: name, argc, parameters, tag, flags
      10,    1,   82,    2, 0x02 /* Public */,
      12,    1,   85,    2, 0x02 /* Public */,
      14,    1,   88,    2, 0x02 /* Public */,
      16,    1,   91,    2, 0x02 /* Public */,

 // signals: parameters
    QMetaType::Void,
    QMetaType::Void,
    QMetaType::Void,
    QMetaType::Void,
    QMetaType::Void,
    QMetaType::Void,

 // slots: parameters
    QMetaType::Void,
    QMetaType::Void,

 // methods: parameters
    QMetaType::Void, QMetaType::Int,   11,
    QMetaType::Void, QMetaType::QString,   13,
    QMetaType::Void, QMetaType::QString,   15,
    QMetaType::Void, QMetaType::Float,   17,

 // properties: name, type, flags
      18, QMetaType::Int, 0x00495103,
      19, QMetaType::QString, 0x00495103,
      20, QMetaType::QString, 0x00495003,
      21, QMetaType::Int, 0x00495001,
      22, QMetaType::Float, 0x00495003,
      23, QMetaType::Float, 0x00495003,

 // properties: notify_signal_id
       0,
       1,
       2,
       3,
       5,
       4,

       0        // eod
};

void CarInfoController::qt_static_metacall(QObject *_o, QMetaObject::Call _c, int _id, void **_a)
{
    if (_c == QMetaObject::InvokeMetaMethod) {
        auto *_t = static_cast<CarInfoController *>(_o);
        Q_UNUSED(_t)
        switch (_id) {
        case 0: _t->batteryLevelChanged(); break;
        case 1: _t->modeColorChanged(); break;
        case 2: _t->directionChanged(); break;
        case 3: _t->batteryValueChanged(); break;
        case 4: _t->distValueChanged(); break;
        case 5: _t->distanceChanged(); break;
        case 6: _t->getBattery(); break;
        case 7: _t->generateRandomDistance(); break;
        case 8: _t->setBatteryLevel((*reinterpret_cast< int(*)>(_a[1]))); break;
        case 9: _t->setModeColor((*reinterpret_cast< const QString(*)>(_a[1]))); break;
        case 10: _t->setDirection((*reinterpret_cast< const QString(*)>(_a[1]))); break;
        case 11: _t->setDistance((*reinterpret_cast< float(*)>(_a[1]))); break;
        default: ;
        }
    } else if (_c == QMetaObject::IndexOfMethod) {
        int *result = reinterpret_cast<int *>(_a[0]);
        {
            using _t = void (CarInfoController::*)();
            if (*reinterpret_cast<_t *>(_a[1]) == static_cast<_t>(&CarInfoController::batteryLevelChanged)) {
                *result = 0;
                return;
            }
        }
        {
            using _t = void (CarInfoController::*)();
            if (*reinterpret_cast<_t *>(_a[1]) == static_cast<_t>(&CarInfoController::modeColorChanged)) {
                *result = 1;
                return;
            }
        }
        {
            using _t = void (CarInfoController::*)();
            if (*reinterpret_cast<_t *>(_a[1]) == static_cast<_t>(&CarInfoController::directionChanged)) {
                *result = 2;
                return;
            }
        }
        {
            using _t = void (CarInfoController::*)();
            if (*reinterpret_cast<_t *>(_a[1]) == static_cast<_t>(&CarInfoController::batteryValueChanged)) {
                *result = 3;
                return;
            }
        }
        {
            using _t = void (CarInfoController::*)();
            if (*reinterpret_cast<_t *>(_a[1]) == static_cast<_t>(&CarInfoController::distValueChanged)) {
                *result = 4;
                return;
            }
        }
        {
            using _t = void (CarInfoController::*)();
            if (*reinterpret_cast<_t *>(_a[1]) == static_cast<_t>(&CarInfoController::distanceChanged)) {
                *result = 5;
                return;
            }
        }
    }
#ifndef QT_NO_PROPERTIES
    else if (_c == QMetaObject::ReadProperty) {
        auto *_t = static_cast<CarInfoController *>(_o);
        Q_UNUSED(_t)
        void *_v = _a[0];
        switch (_id) {
        case 0: *reinterpret_cast< int*>(_v) = _t->batteryLevel(); break;
        case 1: *reinterpret_cast< QString*>(_v) = _t->modeColor(); break;
        case 2: *reinterpret_cast< QString*>(_v) = _t->blinkDirection(); break;
        case 3: *reinterpret_cast< int*>(_v) = _t->batteryValue(); break;
        case 4: *reinterpret_cast< float*>(_v) = _t->distanceCM(); break;
        case 5: *reinterpret_cast< float*>(_v) = _t->distanceValue(); break;
        default: break;
        }
    } else if (_c == QMetaObject::WriteProperty) {
        auto *_t = static_cast<CarInfoController *>(_o);
        Q_UNUSED(_t)
        void *_v = _a[0];
        switch (_id) {
        case 0: _t->setBatteryLevel(*reinterpret_cast< int*>(_v)); break;
        case 1: _t->setModeColor(*reinterpret_cast< QString*>(_v)); break;
        case 2: _t->setDirection(*reinterpret_cast< QString*>(_v)); break;
        case 4: _t->setDistance(*reinterpret_cast< float*>(_v)); break;
        case 5: _t->setDistance(*reinterpret_cast< float*>(_v)); break;
        default: break;
        }
    } else if (_c == QMetaObject::ResetProperty) {
    }
#endif // QT_NO_PROPERTIES
}

QT_INIT_METAOBJECT const QMetaObject CarInfoController::staticMetaObject = { {
    &QObject::staticMetaObject,
    qt_meta_stringdata_CarInfoController.data,
    qt_meta_data_CarInfoController,
    qt_static_metacall,
    nullptr,
    nullptr
} };


const QMetaObject *CarInfoController::metaObject() const
{
    return QObject::d_ptr->metaObject ? QObject::d_ptr->dynamicMetaObject() : &staticMetaObject;
}

void *CarInfoController::qt_metacast(const char *_clname)
{
    if (!_clname) return nullptr;
    if (!strcmp(_clname, qt_meta_stringdata_CarInfoController.stringdata0))
        return static_cast<void*>(this);
    return QObject::qt_metacast(_clname);
}

int CarInfoController::qt_metacall(QMetaObject::Call _c, int _id, void **_a)
{
    _id = QObject::qt_metacall(_c, _id, _a);
    if (_id < 0)
        return _id;
    if (_c == QMetaObject::InvokeMetaMethod) {
        if (_id < 12)
            qt_static_metacall(this, _c, _id, _a);
        _id -= 12;
    } else if (_c == QMetaObject::RegisterMethodArgumentMetaType) {
        if (_id < 12)
            *reinterpret_cast<int*>(_a[0]) = -1;
        _id -= 12;
    }
#ifndef QT_NO_PROPERTIES
    else if (_c == QMetaObject::ReadProperty || _c == QMetaObject::WriteProperty
            || _c == QMetaObject::ResetProperty || _c == QMetaObject::RegisterPropertyMetaType) {
        qt_static_metacall(this, _c, _id, _a);
        _id -= 6;
    } else if (_c == QMetaObject::QueryPropertyDesignable) {
        _id -= 6;
    } else if (_c == QMetaObject::QueryPropertyScriptable) {
        _id -= 6;
    } else if (_c == QMetaObject::QueryPropertyStored) {
        _id -= 6;
    } else if (_c == QMetaObject::QueryPropertyEditable) {
        _id -= 6;
    } else if (_c == QMetaObject::QueryPropertyUser) {
        _id -= 6;
    }
#endif // QT_NO_PROPERTIES
    return _id;
}

// SIGNAL 0
void CarInfoController::batteryLevelChanged()
{
    QMetaObject::activate(this, &staticMetaObject, 0, nullptr);
}

// SIGNAL 1
void CarInfoController::modeColorChanged()
{
    QMetaObject::activate(this, &staticMetaObject, 1, nullptr);
}

// SIGNAL 2
void CarInfoController::directionChanged()
{
    QMetaObject::activate(this, &staticMetaObject, 2, nullptr);
}

// SIGNAL 3
void CarInfoController::batteryValueChanged()
{
    QMetaObject::activate(this, &staticMetaObject, 3, nullptr);
}

// SIGNAL 4
void CarInfoController::distValueChanged()
{
    QMetaObject::activate(this, &staticMetaObject, 4, nullptr);
}

// SIGNAL 5
void CarInfoController::distanceChanged()
{
    QMetaObject::activate(this, &staticMetaObject, 5, nullptr);
}
QT_WARNING_POP
QT_END_MOC_NAMESPACE
