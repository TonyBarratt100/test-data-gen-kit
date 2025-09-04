def main():
    from tdg.cli import _generate_all
    u,p,o,r=_generate_all(10,5,20,15,seed=123)
    print(u.head()); print(p.head()); print(o.head()); print(r.head())
    print("OK")
if __name__=="__main__": main()
