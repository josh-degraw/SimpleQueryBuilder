module SimpleQueryBuilder.Postgres.Tests.QueryBuilderTests

open System.Text.RegularExpressions
open SimpleQueryBuilder.Postgres
open SimpleQueryBuilder.Core
open Swensen.Unquote
open AutoFixture.Community.FSharp
open Xunit

let private normalizeString (str: string) =
    let reg = Regex ("""\s{2,}""", RegexOptions.Multiline)
    reg.Replace (str.Trim (), " ")

[<Fact>]
let ``where appends new entries`` () =
    let a, b = randVal<string> (), randValsN<string> (4)

    let query =
        postgresQuery {
            where a
            where b
        }

    query.Where |> Seq.toList =! [ a; yield! b ]

[<Fact>]
let ``orderBy appends orders by the given property`` () =

    let query =
        postgresQuery {
            select "a" "b" "5 + 8 as calculated"
            from "table_a" "table_b"
            where "b is null" "a > 2"
            orderBy "b"
        }
        |> SqlQuery.toString
        |> normalizeString

    test
        <@ query = normalizeString
                       """
                select a, b, 5 + 8 as calculated
                from table_a, table_b
                where (b is null) and (a > 2)
                order by b asc;
            """ @>

[<Fact>]
let ``orderBy appends orders by the given properties`` () =

    let noneOptional: string option = None
    let someOptional = Some "calculated"

    let query =
        postgresQuery {
            select "a" "b" "5 + 8 as calculated"

            from "table_a" "table_b"
            where "b is null" "a > 2"
            orderBy "b"
            orderBy noneOptional
            orderByDescending someOptional
        }
        |> SqlQuery.toString
        |> normalizeString

    test
        <@ query = normalizeString
                       """
                select a, b, 5 + 8 as calculated
                from table_a, table_b
                where (b is null) and (a > 2)
                order by b asc, calculated desc;
            """ @>

[<Fact>]
let ``query to sql renders correctly`` () =
    let query =
        postgresQuery {
            select "a" "b" "5 + 8 as calculated"

            from "table_a" "table_b"
            where "b is null" "a > 2"
        }
        |> SqlQuery.toString
        |> normalizeString


    let expected =
        normalizeString
            """
                select a, b, 5 + 8 as calculated
                from table_a, table_b
                where (b is null) and (a > 2);
            """
    query =! expected

let randQuery () =
    { PostgresQuery.empty with
        Select = randValsN 3
        Where = randValsN 3
        From = randValsN 3
    }

[<Fact>]
let ``extend applies the combinators in the correct order`` () =
    let a =
        postgresQuery {
            select "a" "b" "c"
            from "table_a"
            where "a > 5"
        }

    let b =
        postgresQuery {
            select "d" "e" "f"
            from "table_b"
            where "d < e"
        }

    let merged =
        postgresQuery {
            select a.Select
            from a.From
            where a.Where
            extend b
        }

    merged.Select |> Seq.toList =! [ yield! a.Select; yield! b.Select ]
    merged.From |> Seq.toList =! [ yield! a.From; yield! b.From ]
    merged.Where |> Seq.toList =! [ yield! a.Where; yield! b.Where ]

[<Fact>]
let ``extend applies the combinators in the correct order via static functions`` () =
    let a =
        postgresQuery {
            select "a" "b" "c"
            from "table_a"
            where "a > 5"
        }

    let b =
        postgresQuery {
            select "d" "e" "f"
            from "table_b"
            where "d < e"
        }


    let merged = SqlQuery.extend a b

    merged.Select |> Seq.toList =! [ yield! a.Select; yield! b.Select ]
    merged.From |> Seq.toList =! [ yield! a.From; yield! b.From ]
    merged.Where |> Seq.toList =! [ yield! a.Where; yield! b.Where ]

[<Fact>]
let ``extended query renders sql correctly`` () =
    let a =
        postgresQuery {
            select "a" "b" "c"
            from "table_a"
            where "a > 5"
        }

    let b =
        postgresQuery {
            select "d" "e" "f"
            from "table_b"
            where "d < e"
        }

    let merged =
        postgresQuery {
            select a.Select
            from a.From
            where a.Where
            extend b
        }
        |> SqlQuery.toString

    let expected =
        postgresQuery {
            select [ yield! a.Select
                     yield! b.Select ]

            from [ yield! a.From; yield! b.From ]
            where [ yield! a.Where; yield! b.Where ]
        }
        |> SqlQuery.toString

    merged =! expected


[<Fact>]
let ``extend applies the combinators in the correct order when extend comes first`` () =
    let a =
        postgresQuery {
            select "a" "b" "c"
            from "table_a"
            where "a > 5"
        }

    let b =
        postgresQuery {
            select "d" "e" "f"
            from "table_b"
            where "d < e"
        }

    let merged =
        postgresQuery {
            extend b
            select a.Select
            from a.From
            where a.Where
        }

    merged.Select |> Seq.toList =! [ yield! b.Select; yield! a.Select ]
    merged.From |> Seq.toList =! [ yield! b.From; yield! a.From ]
    merged.Where |> Seq.toList =! [ yield! b.Where; yield! a.Where ]

[<Fact>]
let ``whereSome should not apply the given filter if the value is None`` () =
    let query =
        postgresQuery {
            select "column"
            from "table"
            whereSome None "this should not be found"
        }
        |> SqlQuery.toString
        |> normalizeString

    query =! "select column from table;"

[<Fact>]
let ``whereSome should apply the given filter if the value is Some`` () =
    let query =
        postgresQuery {
            select "column"
            from "table"
            whereSome (Some 8) "value = 8"
        }
        |> SqlQuery.toString
        |> normalizeString

    query =! "select column from table where (value = 8);"

[<Fact>]
let ``count overwrites any existing columns`` () =
    let beforeQuery = randQuery ()

    test <@ beforeQuery.Select |> Seq.length > 1 @>
    let query = beforeQuery |> SqlQuery.countAll

    query.Select |> Seq.toList =! [ "count(*)" ]

[<Fact>]
let ``paginate renders correct sql`` () =
    let paginationCriteria: PaginationCriteria = randVal ()

    let query =
        postgresQuery {
            select "column"
            from "table"
            paginate paginationCriteria

        }
        |> SqlQuery.toString
        |> normalizeString

    query
    =! normalizeString (
        sprintf "select column from table %s;" (paginationCriteria |> postgresQueryTranslator.pagination)
    )

[<Fact>]
let ``query with from and join renders correctly`` () =
    let query =
        postgresQuery {
            select "t.column"
            from "table as t"
            innerJoin "other_table o on o.id = t.other_id"
        }
        |> SqlQuery.toString
        |> normalizeString

    query
    =! normalizeString
        """
            select t.column
            from table as t
            inner join other_table o on o.id = t.other_id;
        """

[<Fact>]
let ``query with multiple from and a single join renders correctly`` () =
    let query =
        postgresQuery {
            select "t.column"
            from "table as t" "table_2 as tt"
            innerJoin "other_table o on o.id = t.other_id"
        }
        |> SqlQuery.toString
        |> normalizeString

    query
    =! normalizeString
        """
            select t.column
            from table as t, table_2 as tt
            inner join other_table o on o.id = t.other_id;
        """

[<Fact>]
let ``query with single from and multiple joins renders correctly`` () =
    let query =
        postgresQuery {
            select "t.column"
            from "table as t"
            innerJoin "other_table o on o.id = t.other_id"
            innerJoin "table_2 tt on tt.id = t.tt_id"
        }
        |> SqlQuery.toString
        |> normalizeString

    query
    =! normalizeString
        """
            select t.column
            from table as t
            inner join other_table o on o.id = t.other_id
            inner join table_2 tt on tt.id = t.tt_id;
        """

[<Fact>]
let ``query with multiple from and multiple joins renders correctly`` () =
    let query =
        postgresQuery {
            select "t.column"
            from "table as t" "table_2 as tt"
            innerJoin "other_table o on o.id = t.other_id"
            leftJoin "table_3 th on th.id = o.th_id"
        }
        |> SqlQuery.toString
        |> normalizeString

    query
    =! normalizeString
        """
            select t.column
            from table as t, table_2 as tt
            inner join other_table o on o.id = t.other_id
            left join table_3 th on th.id = o.th_id;
        """

[<Fact>]
let ``query with cte renders correctly`` () =
    let query =
        postgresQuery {
            withCte "c as (select x from table_x where x is not null)"

            select "a" "b" "5 + 8 as calculated"

            from "table_a" "table_b"
            where "b is null" "a > 2"
        }
        |> SqlQuery.toString
        |> normalizeString

    let expected =
        normalizeString
            """
                with c as (select x from table_x where x is not null)
                select a, b, 5 + 8 as calculated
                from table_a, table_b
                where (b is null) and (a > 2);
            """

    query =! expected

[<Fact>]
let ``query with multiple ctes renders correctly`` () =
    let query =
        postgresQuery {
            withCte "c as (select x from table_x where x is not null)" "d as (select y from table_y)"
            select "a" "b" "5 + 8 as calculated"
            from "table_a" "table_b"
            whereNone None "b is null"
            where "a > 2"

        }
        |> SqlQuery.toString
        |> normalizeString

    let expected =
        normalizeString
            """
                with c as (select x from table_x where x is not null), d as (select y from table_y)
                select a, b, 5 + 8 as calculated
                from table_a, table_b
                where (b is null) and (a > 2);
            """

    query =! expected
