library('ggplot2')
library('scales')
df <- read.csv('input/lastfm_varying_dim.csv', header=FALSE, col.names=c('prc', 'k', 'value'))
str(df)

plot <- ggplot(data = df, aes(x = k, y = value, group = prc, colour = prc))
plot <- plot + geom_point()
plot <- plot + geom_line()

plot <- plot + labs(y = 'Running time (s)', x = 'k', colour = 'Percentage')

ggsave("lastfm_varying_dim.png")
