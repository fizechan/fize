from functools import wraps


class Logit:

    logfile = None

    def __init__(self, logfile="log.log"):
        self.logfile = logfile

    @staticmethod
    def notify(self):
        """发送提醒"""
        pass

    def __call__(self, func):
        @wraps(func)
        def decorator_fun(*args, **kwargs):
            log_string = func.__name__ + " was called"
            print(log_string)
            # 打开logfile并写入
            with open(self.logfile, 'a') as opened_file:
                # 现在将日志打到指定的文件
                opened_file.write(log_string + '\n')
            # 现在，发送一个通知
            self.notify(self)
            return func(*args, **kwargs)

        return decorator_fun
