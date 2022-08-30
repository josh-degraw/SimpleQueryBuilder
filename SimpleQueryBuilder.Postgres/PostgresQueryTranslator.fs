module SimpleQueryBuilder.Postgres

open System
open SimpleQueryBuilder.Core
open SimpleQueryBuilder.Core.Util

let private withCte query =
    query.With
    |> String.concat ", "
    |> function
        | String.NotNullOrWhiteSpace s -> sprintf "with %s" s
        | _ -> String.Empty

let private select query =
    query.Select |> String.concat ", " |> sprintf "select %s"

let private from query =
    query.From |> String.concat ", " |> sprintf "from %s"

let private joins query =
    query.Joins |> String.concat Environment.NewLine

let private where query =
    query.Where
    |> Seq.filter (String.IsNullOrWhiteSpace >> not)
    |> Seq.map (sprintf "(%s)")
    |> String.concat " and "
    |> function
        | String.NotNullOrWhiteSpace s -> sprintf "where %s" s
        | _ -> String.Empty

let private orderBy query =
    query.Sorting
    |> Seq.map string
    |> String.concat ", "
    |> function
        | String.NotNullOrWhiteSpace s -> sprintf "order by %s" s
        | _ -> String.Empty

let private groupBy query =
    query.GroupBy
    |> String.concat ", "
    |> function
        | String.NotNullOrWhiteSpace s -> sprintf "group by %s" s
        | _ -> String.Empty


/// Overwrites any columns previously specified.
let private count = sprintf "count(%s)"

/// Includes the total count of all rows matching the query (regardless of paging) as an additional column
let private includeCount = sprintf "count(%s) over() as total_count"

let private pagination criteria =
    let getOffset criteria =
        criteria.Page * criteria.Size

    seq {
        if criteria.Size > 0 then "limit @pageSize"
        if criteria |> getOffset > 0 then "offset @offset"
    }
    |> String.concat " "

let private limit query =
    match query.Pagination with
    | Some criteria -> pagination criteria
    | None -> String.Empty

let postgresQueryTranslator =
    {
        withCte = withCte
        select = select
        from = from
        joins = joins
        where = where
        orderBy = orderBy
        groupBy = groupBy
        limit = limit
        count = count
        includeCount = includeCount
        pagination = pagination
    }

module PostgresQuery =
    let empty = { SqlQuery.empty with Translator = postgresQueryTranslator }

let postgresQuery = SqlQueryBuilder postgresQueryTranslator
