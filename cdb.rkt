#lang racket

;
;  This is an early alpha version; don't use it,
;  because I don't know what I am doing!
;

(require ffi/unsafe)

(define libcdb (ffi-lib "libcdb"))

(define get-port-fd
  (get-ffi-obj "scheme_get_port_fd" #f
               (_fun _scheme -> _int)))


; Maker

(define-cstruct _cdb-make ([fd    _int]
                           [dpos  _uint]
                           [rcnt  _uint]
                           [buf   _bytes] ; allocate [4096]
                           [bpos  _pointer]
                           [rec   _bytes] ; allocate [256]
                           ))

(define cdb-make-start-ffi
  (get-ffi-obj "cdb_make_start" libcdb 
               (_fun _cdb-make-pointer _int -> _bool)))

(define new-cdb-make 
  (make-cdb-make 0 0 0 (make-bytes 4096) #f (make-bytes 256)))

(define (cdb-make-start port)
  (let ([c new-cdb-make])
    (cdb-make-start-ffi c (get-port-fd port))
    c))

(define cdb-make-finish
  (get-ffi-obj "cdb_make_finish" libcdb
               (_fun _cdb-make-pointer -> _bool)))

(define cdb-make-add-ffi
  (get-ffi-obj "cdb_make_add" libcdb
               (_fun _cdb-make-pointer _string/utf-8 
                     _uint _string/utf-8 _uint -> _bool)))

(define (cdb-make-add cdb key value)
  (not (cdb-make-add-ffi cdb key (string-utf-8-length key)
                         value (string-utf-8-length value))))


; Reader

(define-cstruct _cdb ([fd    _int]
                      [fsize _uint]
                      [dend  _uint]
                      [mem   _pointer]
                      [vpos  _uint]
                      [vlen  _uint]
                      [kpos  _uint]
                      [klen  _uint]))

(define new-cdb
  (make-cdb 0 0 0 #f 0 0 0 0))

(define cdb-init-ffi
  (get-ffi-obj "cdb_init" libcdb 
               (_fun _cdb-pointer _int -> _bool)))

(define (cdb-init port)
  (let ([c new-cdb])
    (cdb-init-ffi c (get-port-fd port))
    c))

(define cdb-find-ffi
  (get-ffi-obj "cdb_find" libcdb
               (_fun _cdb-pointer _string/utf-8 _uint -> _bool)))

(define (cdb-find cdb key)
  (not (cdb-find-ffi cdb key (string-utf-8-length key))))

(define (cdb-datalen c)
  (cdb-vlen c))

(define (cdb-datapos c)
  (cdb-vpos c))

(define cdb-read-ffi
  (get-ffi-obj "cdb_read" libcdb
               (_fun _cdb-pointer _bytes _uint _uint -> _bool)))

(define (cdb-read cdb)
  (let* ([vpos (cdb-datapos cdb)]
         [vlen (cdb-datalen cdb)]
         [outs (make-bytes vlen)])
    (cdb-read-ffi cdb outs vlen vpos)
    (bytes->string/utf-8 outs)))

(define (cdb-get-value cdb key)
  (cdb-find cdb key)
  (cdb-read cdb))

(define-cstruct _cdb-find-st ([cdb    _pointer]
                              [hval   _uint]
                              [htp    _pointer]
                              [htab   _pointer]
                              [htend  _pointer]
                              [httodo _uint]
                              [key    _pointer]
                              [klen   _uint]))
(define new-cdb-find-st
  (make-cdb-find-st #f 0 #f #f #f 0 #f 0))

(define cdb-findinit-ffi
  (get-ffi-obj "cdb_findinit" libcdb
               (_fun _cdb-find-st-pointer _cdb-pointer 
                     _string/utf-8 _uint -> _void)))

(define cdb-findnext
  (get-ffi-obj "cdb_findnext" libcdb
               (_fun _cdb-find-st-pointer -> _bool)))

(define (cdb-findinit cdb key) 
  (let ([cf new-cdb-find-st])
    (cdb-findinit-ffi cf cdb key (string-utf-8-length key))
    cf))

(define (cdb-get-values cdb key)
  (let ([cf (cdb-findinit cdb key)])
    (define (next-value lst)
      (if (cdb-findnext cf)
          (cons (cdb-read cdb) (next-value lst))
          lst))
    (next-value empty)))

; Better functions

(define (call-with-cdb-reader file proc)
  (call-with-continuation-barrier
   (lambda ()
     (let* ([port (open-input-file file)]
            [cdb (cdb-init port)])
       (dynamic-wind
        void
        (lambda ()
          (proc cdb))
        (lambda ()
          (close-input-port port)))))))

; Example:
;
;(call-with-cdb-reader "/Users/dmitry/Desktop/test.txt"
;                        (lambda (cdb)
;                          (cdb-get-value cdb "Hello")))