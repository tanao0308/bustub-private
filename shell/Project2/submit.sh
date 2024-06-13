cd ../..
rm GRADESCOPE.md
cd build
make submit-p2
cd ..
python3 gradescope_sign.py
mv project2-submission.zip /mnt/d/Desktop
