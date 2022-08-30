[<RequireQualifiedAccess>]
module SimpleQueryBuilder.Core.Util.String

open System
open System.Globalization
open System.Runtime.CompilerServices
open System.Text

let lower (str: string) = str.ToLower()

let replace (oldValue: string) (newValue: string) (str: string) = str.Replace(oldValue, newValue)

let trim (str: string) = str.Trim()

let (|NullOrWhiteSpace|NotNullOrWhiteSpace|) str =
    if str |> String.IsNullOrWhiteSpace then
        NullOrWhiteSpace
    else
        NotNullOrWhiteSpace str

let (|NullOrEmpty|NotNullOrEmpty|) str =
    if str |> String.IsNullOrEmpty then
        NullOrEmpty
    else
        NotNullOrEmpty str

let toOption =
    function
    | NotNullOrEmpty str -> Some str
    | NullOrEmpty -> None

// Adapted from https://github.com/efcore/EFCore.NamingConventions/blob/main/EFCore.NamingConventions/Internal/SnakeCaseNameRewriter.cs
let toSnakeCase str =
    match str with
    | NullOrEmpty -> str
    | NotNullOrEmpty str ->
        let builder =
            StringBuilder(str.Length + Math.Min(2, str.Length / 5))

        let mutable previousCategory = None

        let append (character: char) = builder.Append character |> ignore

        for currentIndex = 0 to str.Length - 1 do
            let currentChar = str.[currentIndex]

            let currentCategory =
                Char.GetUnicodeCategory currentChar

            match currentChar, currentCategory with
            | '_', _ ->
                append '_'
                previousCategory <- None
            | _, UnicodeCategory.UppercaseLetter
            | _, UnicodeCategory.TitlecaseLetter ->
                if previousCategory = Some UnicodeCategory.SpaceSeparator
                   || previousCategory = Some UnicodeCategory.LowercaseLetter
                   || previousCategory
                      <> Some UnicodeCategory.DecimalDigitNumber
                      && previousCategory <> None
                      && currentIndex > 0
                      && currentIndex + 1 < str.Length
                      && Char.IsLower str.[currentIndex + 1] then
                    append '_'

                Char.ToLower(currentChar, CultureInfo.InvariantCulture)
                |> append

                previousCategory <- Some currentCategory
            | _, UnicodeCategory.LowercaseLetter
            | _, UnicodeCategory.DecimalDigitNumber ->
                if previousCategory = Some UnicodeCategory.SpaceSeparator then
                    append '_'

                builder.Append currentChar |> ignore
                previousCategory <- Some currentCategory
            | _ ->
                if previousCategory <> None then
                    previousCategory <- Some UnicodeCategory.SpaceSeparator

        builder.ToString()

[<Extension>]
type StringExtension =
    [<Extension>]
    static member ToDateTimeOffsetOption str =
        if String.IsNullOrWhiteSpace str then
            None
        else
            let isValid, validDate =
                DateTimeOffset.TryParse str

            match isValid with
            | true -> Some validDate
            | false -> None
