cd ../..
rm GRADESCOPE.md
cd build
make submit-p1
cd ..
python3 gradescope_sign.py
mv project1-submission.zip /mnt/d/Desktop
