import poplib

def del_email():
    uugame = poplib.POP3('pop3.mxhichina.com',110)
    uugame.user('cop@quyiyuan.com')
    uugame.pass_('****')
    totalNum,totalSize = uugame.stat()
    print "total=" + str(totalNum)
    for i in range(1000):
        uugame.dele(i+1)
    uugame.quit()

if __name__ == '__main__':
    del_email()