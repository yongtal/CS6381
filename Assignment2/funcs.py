#if idk lies in a left open right close interval

def in_lorc(idk,s,t,MOD):
  #print "in lorc: ", idk, s, t
  if s <= t:
    return idk > s and idk <= t 
  else :
    return in_lorc(idk,s,MOD,MOD) or in_lorc(idk,0,t,MOD)

def in_loro(idk,s,t,MOD):
  if s <= t:
    return idk > s and idk < t 
  elif s > t:
    return in_loro(idk,s,MOD+1,MOD) or in_loro(idk,0,t,MOD)
