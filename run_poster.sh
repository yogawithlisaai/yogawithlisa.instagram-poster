#!/bin/bash
cd /Users/lisa/Documents/GitHub/yogawithlisa.instagram-poster
source .venv/bin/activate
/usr/bin/python3 post_to_instagram.py --limit 1 >> daily_post_log.txt 2>&1
