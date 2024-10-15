#!/bin/bash

bin/start-uc-server
bin/uc catalog create --name bronze
bin/uc schema create --catalog bronze --name test