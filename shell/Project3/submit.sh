cd ../..
rm GRADESCOPE.md
cd build
make submit-p3
cd ..
python3 gradescope_sign.py
mv project3-submission.zip /mnt/c/Users/shicaolong/Desktop
