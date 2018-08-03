#!/bin/bash

# Create tables
beeline -u jdbc:hive2://master:10000 -f create_tables.hql