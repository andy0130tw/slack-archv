def ts_upper_bound(l, ts):
    L = 0; R = len(l);
    while L < R:
        M = L + (R-L)//2
        if l[M]['timestamp'] <= ts:
            L = M+1
        elif l[M]['timestamp'] > ts:
            R = M

    return L
