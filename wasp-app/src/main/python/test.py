x = 10          # 전역 변수
def foo():
    global x    # 전역 변수 x를 사용하겠다고 설정
    x = 20      # x는 전역 변수
    print(x)    # 전역 변수 출력

def abc():
    print(x)

if __name__ == "__main__":
    foo()
    foo()
    abc()
    print(x)        # 전역 변수 출력