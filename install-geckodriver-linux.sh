#!/bin/bash

wget https://github.com/mozilla/geckodriver/releases/download/v0.30.0/geckodriver-v0.30.0-linux32.tar.gz
tar -xf geckodriver-v0.30.0-linux32.tar.gz
mv geckodriver /usr/local/bin
chmod +x /usr/local/bin/geckodriver
