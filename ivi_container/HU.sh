#!/usr/bin/env bash

COMMONAPI_CONFIG=commonapi4someip.ini LD_LIBRARY_PATH=./install_folder/lib:$PWD/./Head_Unit/v1.0.0/build-Head_Unit_app-Desktop-Debug VSOMEIP_CONFIGURATION=./json/HU.json VSOMEIP_APPLICATION_NAME=Head_Unit_app ./Head_Unit/v1.0.0/build-Head_Unit_app-Desktop-Debug/Head_Unit_app

