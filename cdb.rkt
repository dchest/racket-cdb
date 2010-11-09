#lang racket

;
;  This is an early alpha version, don't use,
;  because I don't know what I am doing!
;

(require ffi/unsafe)

(define libcdb (ffi-lib "libcdb"))

(define get-port-fd
  (get-ffi-obj "scheme_get_port_fd" #f
               (_fun _scheme -> _int)))


; Maker

(define-cstruct _cdb-make ([cdb_fd    _int]
                           [cdb_dpos  _uint]
                           [cdb_rcnt  _uint]
                           [cdb_buf   _bytes] ; allocate [4096]
                           [cdb_bpos  _pointer]
                           [cdb_rec   _bytes] ; allocate [256]
                           ))

(define cdb-make-start
  (get-ffi-obj "cdb_make_start" libcdb 
               (_fun _cdb-make-pointer _int -> _bool)))

(define cdb-make-finish
  (get-ffi-obj "cdb_make_finish" libcdb
               (_fun _cdb-make-pointer -> _bool)))

(define new-cdb-make 
  (make-cdb-make 0 0 0 (make-bytes 4096) #f (make-bytes 256)))

(define cdb-make-add
  (get-ffi-obj "cdb_make_add" libcdb
               (_fun _cdb-make-pointer _string/utf-8 
                     _uint _string/utf-8 _uint -> _bool)))

(define (cdb-make-add-easy cdb key value)
  (not (cdb-make-add cdb key (string-utf-8-length key)
                     value (string-utf-8-length value))))

(define (cdb-make-start-easy port)
  (let ([c new-cdb-make])
    (cdb-make-start c (get-port-fd port))
    c))


; Reader

(define-cstruct _cdb ([cdb_fd    _int]
                      [cdb_fsize _uint]
                      [cdb_dend  _uint]
                      [cdb_mem   _pointer]
                      [cdb_vpos  _uint]
                      [cdb_vlen  _uint]
                      [cdb_kpos  _uint]
                      [cdb_klen  _uint]))

(define new-cdb
  (make-cdb 0 0 0 #f 0 0 0 0))

(define cdb-init
  (get-ffi-obj "cdb_init" libcdb 
               (_fun _cdb-pointer _int -> _bool)))

(define (cdb-init-easy port)
  (let ([c new-cdb])
    (cdb-init c (get-port-fd port))
    c))

(define cdb-find
  (get-ffi-obj "cdb_find" libcdb
               (_fun _cdb-pointer _string/utf-8 _uint -> _bool)))

(define (cdb-find-easy cdb key)
  (not (cdb-find cdb key (string-utf-8-length key))))

(define cdb-read
  (get-ffi-obj "cdb_read" libcdb
               (_fun _cdb-pointer _bytes _uint _uint -> _bool)))

(define (cdb-datalen c)
  (cdb-cdb_vlen c))

(define (cdb-datapos c)
  (cdb-cdb_vpos c))

(define (cdb-read-easy cdb)
  (let* ([vpos (cdb-datapos cdb)]
         [vlen (cdb-datalen cdb)]
         [outs (make-bytes vlen)])
    (cdb-read cdb outs vlen vpos)
    (bytes->string/utf-8 outs)))

(define (cdb-get-value cdb key)
  (cdb-find-easy cdb key)
  (cdb-read-easy cdb))

; Better functions

(define (call-with-cdb-reader file proc)
  (call-with-continuation-barrier
   (lambda ()
     (let* ([port (open-input-file file)]
            [cdb (cdb-init-easy port)])
       (dynamic-wind
        void
        (lambda ()
          (proc cdb))
        (lambda ()
          (close-input-port port)))))))

;(define out (open-input-file "/Users/dmitry/Desktop/test.txt"))
;(define fd (get-port-fd out))
;> (define cdb new-cdb-make)
;> cdb
;#<cpointer:cdb-make>
;> (cdb-make-start cdb (get-port-fd out))
;> cdb
;#<cpointer:cdb-make>

;> (define out (open-output-file "/Users/dmitry/Desktop/test.txt"))
;> (define cdb new-cdb-make)
;> (cdb-make-start cdb (get-port-fd out))
;0
;> (cdb-make-add-easy cdb "Hello" "World")
;0
;> (cdb-make-finish cdb)
;0
;> (close-output-port out)

;
;(call-with-cdb-reader "/Users/dmitry/Desktop/test.txt"
;                        (lambda (cdb)
;                          (cdb-get-value cdb "Hello")))