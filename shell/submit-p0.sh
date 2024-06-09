cd ..
rm GRADESCOPE.md
cd build
make submit-p0
cd ..
python3 gradescope_sign.py
