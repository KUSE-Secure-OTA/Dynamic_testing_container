/****************************************************************************
** Meta object code from reading C++ file 'gearController.h'
**
** Created by: The Qt Meta Object Compiler version 67 (Qt 5.12.8)
**
** WARNING! All changes made in this file will be lost!
*****************************************************************************/

#include "gearController.h"
#include <QtCore/qbytearray.h>
#include <QtCore/qmetatype.h>
#if !defined(Q_MOC_OUTPUT_REVISION)
#error "The header file 'gearController.h' doesn't include <QObject>."
#elif Q_MOC_OUTPUT_REVISION != 67
#error "This file was generated using the moc from 5.12.8. It"
#error "cannot be used with the include files from this version of Qt."
#error "(The moc has changed too much.)"
#endif

QT_BEGIN_MOC_NAMESPACE
QT_WARNING_PUSH
QT_WARNING_DISABLE_DEPRECATED
struct qt_meta_stringdata_GearController_t {
    QByteArrayData data[9];
    char stringdata0[126];
};
#define QT_MOC_LITERAL(idx, ofs, len) \
    Q_STATIC_BYTE_ARRAY_DATA_HEADER_INITIALIZER_WITH_OFFSET(len, \
    qptrdiff(offsetof(qt_meta_stringdata_GearController_t, stringdata0) + ofs \
        - idx * sizeof(QByteArrayData)) \
    )
static const qt_meta_stringdata_GearController_t qt_meta_stringdata_GearController = {
    {
QT_MOC_LITERAL(0, 0, 14), // "GearController"
QT_MOC_LITERAL(1, 15, 21), // "selectedButtonChanged"
QT_MOC_LITERAL(2, 37, 0), // ""
QT_MOC_LITERAL(3, 38, 15), // "serverOKChanged"
QT_MOC_LITERAL(4, 54, 17), // "logSelectedButton"
QT_MOC_LITERAL(5, 72, 17), // "setSelectedButton"
QT_MOC_LITERAL(6, 90, 6), // "button"
QT_MOC_LITERAL(7, 97, 14), // "selectedButton"
QT_MOC_LITERAL(8, 112, 13) // "serverOKValue"

    },
    "GearController\0selectedButtonChanged\0"
    "\0serverOKChanged\0logSelectedButton\0"
    "setSelectedButton\0button\0selectedButton\0"
    "serverOKValue"
};
#undef QT_MOC_LITERAL

static const uint qt_meta_data_GearController[] = {

 // content:
       8,       // revision
       0,       // classname
       0,    0, // classinfo
       4,   14, // methods
       2,   40, // properties
       0,    0, // enums/sets
       0,    0, // constructors
       0,       // flags
       2,       // signalCount

 // signals: name, argc, parameters, tag, flags
       1,    0,   34,    2, 0x06 /* Public */,
       3,    0,   35,    2, 0x06 /* Public */,

 // slots: name, argc, parameters, tag, flags
       4,    0,   36,    2, 0x08 /* Private */,

 // methods: name, argc, parameters, tag, flags
       5,    1,   37,    2, 0x02 /* Public */,

 // signals: parameters
    QMetaType::Void,
    QMetaType::Void,

 // slots: parameters
    QMetaType::Void,

 // methods: parameters
    QMetaType::Void, QMetaType::QString,    6,

 // properties: name, type, flags
       7, QMetaType::QString, 0x00495103,
       8, QMetaType::Bool, 0x00495001,

 // properties: notify_signal_id
       0,
       1,

       0        // eod
};

void GearController::qt_static_metacall(QObject *_o, QMetaObject::Call _c, int _id, void **_a)
{
    if (_c == QMetaObject::InvokeMetaMethod) {
        auto *_t = static_cast<GearController *>(_o);
        Q_UNUSED(_t)
        switch (_id) {
        case 0: _t->selectedButtonChanged(); break;
        case 1: _t->serverOKChanged(); break;
        case 2: _t->logSelectedButton(); break;
        case 3: _t->setSelectedButton((*reinterpret_cast< const QString(*)>(_a[1]))); break;
        default: ;
        }
    } else if (_c == QMetaObject::IndexOfMethod) {
        int *result = reinterpret_cast<int *>(_a[0]);
        {
            using _t = void (GearController::*)();
            if (*reinterpret_cast<_t *>(_a[1]) == static_cast<_t>(&GearController::selectedButtonChanged)) {
                *result = 0;
                return;
            }
        }
        {
            using _t = void (GearController::*)();
            if (*reinterpret_cast<_t *>(_a[1]) == static_cast<_t>(&GearController::serverOKChanged)) {
                *result = 1;
                return;
            }
        }
    }
#ifndef QT_NO_PROPERTIES
    else if (_c == QMetaObject::ReadProperty) {
        auto *_t = static_cast<GearController *>(_o);
        Q_UNUSED(_t)
        void *_v = _a[0];
        switch (_id) {
        case 0: *reinterpret_cast< QString*>(_v) = _t->selectedButton(); break;
        case 1: *reinterpret_cast< bool*>(_v) = _t->serverOKValue(); break;
        default: break;
        }
    } else if (_c == QMetaObject::WriteProperty) {
        auto *_t = static_cast<GearController *>(_o);
        Q_UNUSED(_t)
        void *_v = _a[0];
        switch (_id) {
        case 0: _t->setSelectedButton(*reinterpret_cast< QString*>(_v)); break;
        default: break;
        }
    } else if (_c == QMetaObject::ResetProperty) {
    }
#endif // QT_NO_PROPERTIES
}

QT_INIT_METAOBJECT const QMetaObject GearController::staticMetaObject = { {
    &QObject::staticMetaObject,
    qt_meta_stringdata_GearController.data,
    qt_meta_data_GearController,
    qt_static_metacall,
    nullptr,
    nullptr
} };


const QMetaObject *GearController::metaObject() const
{
    return QObject::d_ptr->metaObject ? QObject::d_ptr->dynamicMetaObject() : &staticMetaObject;
}

void *GearController::qt_metacast(const char *_clname)
{
    if (!_clname) return nullptr;
    if (!strcmp(_clname, qt_meta_stringdata_GearController.stringdata0))
        return static_cast<void*>(this);
    return QObject::qt_metacast(_clname);
}

int GearController::qt_metacall(QMetaObject::Call _c, int _id, void **_a)
{
    _id = QObject::qt_metacall(_c, _id, _a);
    if (_id < 0)
        return _id;
    if (_c == QMetaObject::InvokeMetaMethod) {
        if (_id < 4)
            qt_static_metacall(this, _c, _id, _a);
        _id -= 4;
    } else if (_c == QMetaObject::RegisterMethodArgumentMetaType) {
        if (_id < 4)
            *reinterpret_cast<int*>(_a[0]) = -1;
        _id -= 4;
    }
#ifndef QT_NO_PROPERTIES
    else if (_c == QMetaObject::ReadProperty || _c == QMetaObject::WriteProperty
            || _c == QMetaObject::ResetProperty || _c == QMetaObject::RegisterPropertyMetaType) {
        qt_static_metacall(this, _c, _id, _a);
        _id -= 2;
    } else if (_c == QMetaObject::QueryPropertyDesignable) {
        _id -= 2;
    } else if (_c == QMetaObject::QueryPropertyScriptable) {
        _id -= 2;
    } else if (_c == QMetaObject::QueryPropertyStored) {
        _id -= 2;
    } else if (_c == QMetaObject::QueryPropertyEditable) {
        _id -= 2;
    } else if (_c == QMetaObject::QueryPropertyUser) {
        _id -= 2;
    }
#endif // QT_NO_PROPERTIES
    return _id;
}

// SIGNAL 0
void GearController::selectedButtonChanged()
{
    QMetaObject::activate(this, &staticMetaObject, 0, nullptr);
}

// SIGNAL 1
void GearController::serverOKChanged()
{
    QMetaObject::activate(this, &staticMetaObject, 1, nullptr);
}
QT_WARNING_POP
QT_END_MOC_NAMESPACE
