# Task 3 | Entity Resolution

## 🎯 Obiectiv

Scopul meu este de a construi
un **identificator tolerant la erori**
pentru a recunoaste intrarile unice si duplicatele din tabel.

## 🔎 Cautarea unui identificator in tabel

Primul pas a fost sa sortez coloanele tabelului in functie de procentul de completare.
**De ce?**
Acest lucru este important, intrucat coloanele cu putine date nu sunt deloc utile in a identifica in mod unic o companie.

O observatie cheie a fost gradul de completare al coloanelor `website_domain` si `website_tld`.
**31894** din cele **33447** de linii (**> 95%**) contin informatii in aceste coloane.

Am sortat tabelul initial dupa aceste doua coloane si am vizualizat datele.
Astfel, am ajuns la concluzia ca cele doua pot forma un "Primary Key" pentru tabel, dar cu o observatie importanta:
- Top-level domain-ul poate coincide cu domeniul website-ului (exemplu: `tmall.com`)
- O companie poate avea mai multe site-uri cu acelasi domeniu de baza dar cu TLD-uri diferite

Din acest motiv, nu este ideal sa se construiasca un identificator unic in functie de combinarea celor doua,
ci mai degraba in functie de **domeniul de baza**
(pe care l-am definit a fi literele din `website_domain` din stanga primului punct).

## Urmatorii pasi

1. **Sortarea si esantionarea tabelului**:
    Am sortat tabelul dupa `domeniul de baza` si am selectat coloanele de interes care pot descrie activitatea fiecarei companii.
    - Printre coloanele filtrate se numara:
      - Numele companiei (am identificat companii cu denumiri diferite dar active in aceleasi zone)
      - `main_business_category`, `main_industry`, `main_sector` (acestea au o **corelatie puternica** si sunt completate simultan)
      - Numerele de telefon si alte URL-uri (care pot identifica in mod unic o companie)

2. **Excluderea coloanelor nesigure**:
   - Data de creare/ultima actualizare a companiei (am intalnit valori distincte pentru acelasi lucru)
   - Numele sau descrierile: o companie poate fi descrisa prin multiple cuvinte

Am ajuns la concluzia ca companiile cu acelasi domeniu de baza au ceva in comun si pot fi considerate duplicate.

> Algoritmul meu ar putea fi imbunatatit prin
> includerea gruparii `main_business_category`-`main_industry`-`main_sector`,
> dar aceasta **ar complica enorm de mult solutia**,
> iar majoritatea intrarilor acestor coloane coincid.

## ✏️ Calculul rezultatului final

1. **Sortarea tabelului**: Sortez tabelul initial crescator in functie de `domeniul de baza`, lasand liniile vide la final
2. **Crearea unui dictionar**: Creez un dictionar cu domeniul de baza ca cheie si indicii liniilor din tabelul initial ca valori
3. **Potrivirea liniilor incomplete**:
    Pentru fiecare linie cu domeniul de baza vid,
    caut o **potrivire cat mai fidela** cu una dintre indicii deja aflati in dictionar.
    Compararea coloanelor care pot constitui identificatori unici ai unei companii
    (altele decat domeniul de baza, cum ar fi URL-uri si numere de telefon)
    face ca sistemul meu sa fie **tolerant la lipsa de date**

Tabelele rezultate pentru companiile **unice** vor contine
`domeniul de baza` si `indexul` liniei din tabelul initial,
in timp ce tabelele pentru **duplicate** vor contine
`domeniul de baza` si o `lista de indici` ai liniilor din tabelul initial.

## 📄 Input/Output

Programul `main.py` se asteapta sa gaseasca fisierul `veridion_entity_resolution_challenge.snappy.parquet`
in **radacina acestui repository**.

Fisierele obtinute in urma procesarii datelor se vor gasi in directorul `output/`, dupa cum urmeaza:
```sh
❯ tree output              
output
├── duplicates.parquet
└── uniques.parquet
```


## Alte obersvatii

Ce m-a ajutat pe mine foarte mult sa rezolv problema a fost sa vizualizez datele.
Fara ajutorul unor extensii din VS Code pentru **parquet** si **CSV**,
interpretarea tabelelor (**parquet** in special) ar fi fost mult mai anevoioasa.

:) Ma bucur ca am avut oportunitatea sa rezolv astfel de probleme, chiar am avut de invatat :)