from pydantic import BaseModel, Field


class GenreNode(BaseModel):
    id: str
    name: str = Field(alias="genre_label")
    aliases: list[str] = Field(default_factory=list)


class ArtistNode(BaseModel):
    id: str
    name: str
    country: str | None = None
    aliases: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    genres: list[str] = Field(default_factory=list)
    similar_artists: list[str] = Field(default_factory=list)


class AlbumNode(BaseModel):
    id: str
    title: str
    year: str | int | None = None
    artist_id: str


class TrackNode(BaseModel):
    id: str
    title: str
    album_id: str
