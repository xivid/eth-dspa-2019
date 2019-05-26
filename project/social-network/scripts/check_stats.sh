#!/bin/zsh
diff -rupP <(sort -f ../expected-replies-count.txt) <(sort -f ../log/reply-counts.txt) > diff_reply.txt &
diff -rupP <(sort -f ../expected-comments-count.txt) <(sort -f ../log/comment-counts.txt) > diff_comment.txt

