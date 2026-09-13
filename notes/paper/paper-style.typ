#set page(
  paper: "us-letter",
  margin: (top: 0.8in, bottom: 0.75in, left: 0.85in, right: 0.85in),
  header: context [
    #if counter(page).get().first() > 1 {
      align(right)[#text(size: 8pt, fill: luma(100))[#manuscript-title]]
    }
  ],
  footer: context [
    #align(center)[#text(size: 9pt)[#counter(page).display("1")]]
  ],
)

#set text(font: "Libertinus Serif", size: 11pt)
#set par(justify: true, leading: 0.68em, first-line-indent: 1.25em)
#set heading(numbering: none)
#set table(inset: 4pt, stroke: (paint: luma(180), thickness: 0.35pt))
#show table: set text(size: 8.5pt)
#show raw: set text(font: "DejaVu Sans Mono", size: 7pt)

#show heading.where(level: 2): it => block(
  above: 2.1em,
  below: 0.65em,
  breakable: false,
)[
  #set par(first-line-indent: 0pt)
  #text(size: 14pt, weight: "bold")[#it.body]
]

#show heading.where(level: 3): it => block(
  above: 1.5em,
  below: 0.45em,
  breakable: false,
)[
  #set par(first-line-indent: 0pt)
  #text(size: 11.5pt, weight: "semibold")[#it.body]
]

#align(center)[
  #text(size: 19pt, weight: "bold")[#manuscript-title]
  #v(0.25em)
  #text(size: 12pt, style: "italic")[#manuscript-subtitle]
  #v(0.45em)
  #text(size: 9.5pt, fill: luma(90))[#manuscript-date]
]

#v(1em)
