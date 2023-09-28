package com.ironhead.model;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Datasource {
  // CONSTANTS
  public static final String DB_NAME = "music.db";
  public static final String CONNECTION_STRING = "jdbc:sqlite:/home/victorb/Databases/SQLite/Education/Music/" + DB_NAME;

  // Sort order
  public enum SortOrder {
    NONE, BY_ASC, BY_DESC;

    String value() {
     switch (this) {
       case BY_ASC: return "ASC";
       case BY_DESC: return "DESC";
     }
     return null;
    }
  }

  // Albums
  public static final String TABLE_ALBUMS = "albums";
  public enum AlbumColumn {
    ID, NAME, ARTIST;
    String columnName() {
      switch (this) {
        case ID: return "_id";
        case NAME: return "name";
        case ARTIST: return "artist";
      }
      return null;
    }

    int columnIndex() {
      switch (this) {
        case ID: return 1;
        case NAME: return 2;
        case ARTIST: return 3;
      }
      return -1;
    }
  }

  // Artists
  public static final String TABLE_ARTISTS = "artists";

  public enum ArtistColumn {
    ID, NAME;
    String columnName() {
      switch (this) {
        case ID: return "_id";
        case NAME: return "name";
      }
      return null;
    }

    int columnIndex() {
      switch (this) {
        case ID: return 1;
        case NAME: return 2;
      }
      return -1;
    }
  }

  // Songs
  public static final String TABLE_SONGS = "songs";

  public enum SongsColumn {
    ID, TRACK, TITLE, ALBUM;
    String columnName() {
      switch (this) {
        case ID: return "_id";
        case TRACK: return "track";
        case TITLE: return "title";
        case ALBUM: return "album";
      }
      return null;
    }

    int columnIndex() {
      switch (this) {
        case ID: return 1;
        case TRACK: return 2;
        case TITLE: return 3;
        case ALBUM: return 4;
      }
      return -1;
    }
  }

  // Song Artists
  public enum SongArtistColumn {
    ARTIST_NAME, ALBUM_NAME, SONG_TRACK;

    int columnIndex() {
      switch (this) {
        case ARTIST_NAME: return 1;
        case ALBUM_NAME: return 2;
        case SONG_TRACK: return 3;
      }
      return -1;
    }
  }

  // Artists list View
  public static final String TABLE_ARTIST_SONG_VIEW = "artist_list";
  public static final String CREATE_ARTIST_FOR_SONG_VIEW = "CREATE VIEW IF NOT EXISTS " +
   TABLE_ARTIST_SONG_VIEW + " AS SELECT " + TABLE_ARTISTS + "." + ArtistColumn.NAME.columnName() + ", " +
   TABLE_ALBUMS + "." + AlbumColumn.NAME.columnName() + " AS " + SongsColumn.ALBUM.columnName() + ", " +
          TABLE_SONGS + "." + SongsColumn.TRACK.columnName() + ", " + TABLE_SONGS + "." +
          SongsColumn.TITLE.columnName() + " FROM " + TABLE_SONGS +
          " INNER JOIN " + TABLE_ALBUMS + " ON " + TABLE_SONGS + "." + SongsColumn.ALBUM.columnName() +
          " = " + TABLE_ALBUMS + "." + AlbumColumn.ID.columnName() +
          " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + AlbumColumn.ARTIST + " = " +
          TABLE_ARTISTS + "." + ArtistColumn.ID.columnName() +
          " ORDER BY " + TABLE_ARTISTS + "." + ArtistColumn.NAME.columnName() + ", " +
          TABLE_ALBUMS + "." + AlbumColumn.NAME.columnName() + ", " +
          TABLE_SONGS + "." + SongsColumn.TRACK.columnName();

  private Connection connection;

  // METHODS
  public  boolean open() {
    try {
      connection = DriverManager.getConnection(CONNECTION_STRING);
      return true;
    } catch(SQLException error) {
      System.out.println("Couldn't connect to database: " + error.getMessage());
      return false;
    }
  }

  public void  close() {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException error) {
      System.out.println("Couldn't close connection: " + error.getMessage());
    }
  }

  public List<Artist> queryArtists() {
    try(Statement statement = connection.createStatement();
      ResultSet results = statement.executeQuery("SELECT * FROM " + TABLE_ARTISTS)
    ) {
      List<Artist> artists = new ArrayList<>();
      while (results.next()) {
        Artist artist = new Artist(
                results.getInt(ArtistColumn.ID.columnIndex()),
                results.getString(ArtistColumn.NAME.columnIndex())
        );
        artists.add(artist);
      }
      return  artists;
    } catch (SQLException error) {
      System.out.println("Query failed: " + error.getMessage());
      return null;
    }
  }

  public List<Album> queryAlbums(SortOrder sortOrder) {
    StringBuilder stringBuilder = new StringBuilder("SELECT * FROM ");
    stringBuilder.append(TABLE_ALBUMS);
    if (sortOrder != SortOrder.NONE) {
      stringBuilder.append(" ORDER BY ");
      stringBuilder.append(AlbumColumn.NAME.columnName());
      stringBuilder.append(" COLLATE NOCASE ");
      stringBuilder.append(sortOrder.value());
    }

    try(Statement statement = connection.createStatement();
        ResultSet results = statement.executeQuery(stringBuilder.toString())
    ) {
      List<Album> albums = new ArrayList<>();
      while (results.next()) {
        Album album = new Album(
                results.getInt(AlbumColumn.ID.columnIndex()),
                results.getString(AlbumColumn.NAME.columnIndex()),
                results.getInt(AlbumColumn.ARTIST.columnIndex())
        );
        albums.add(album);
      }
      return  albums;
    } catch (SQLException error) {
      System.out.println("Query failed: " + error.getMessage());
      return null;
    }
  }

  public List<String> queryAlbumsForArtist(String artistName, SortOrder sortOrder) {
    // SELECT albums.name FROM albums
    // INNER JOIN artists ON albums.artist = artists._id
    // WHERE artists.name = 'artistName'
    // ORDER BY albums.name COLLATE NOCASE ASC
    StringBuilder stringBuilder = new StringBuilder("SELECT ");
    stringBuilder.append(TABLE_ALBUMS);
    stringBuilder.append(".");
    stringBuilder.append(AlbumColumn.NAME.columnName());
    stringBuilder.append(" AS albumName");
    stringBuilder.append(" FROM ");
    stringBuilder.append(TABLE_ALBUMS);
    stringBuilder.append(" INNER JOIN ");
    stringBuilder.append(TABLE_ARTISTS);
    stringBuilder.append(" ON ");
    stringBuilder.append(TABLE_ALBUMS);
    stringBuilder.append(".");
    stringBuilder.append(AlbumColumn.ARTIST.columnName());
    stringBuilder.append(" = ");
    stringBuilder.append(TABLE_ARTISTS);
    stringBuilder.append(".");
    stringBuilder.append(ArtistColumn.ID.columnName());
    stringBuilder.append(" WHERE ");
    stringBuilder.append(TABLE_ARTISTS);
    stringBuilder.append(".");
    stringBuilder.append(ArtistColumn.NAME.columnName());
    stringBuilder.append(" = '");
    stringBuilder.append(artistName);
    stringBuilder.append("'");

    if (sortOrder != SortOrder.NONE) {
      stringBuilder.append(" ORDER BY ");
      stringBuilder.append(TABLE_ALBUMS);
      stringBuilder.append(".");
      stringBuilder.append(AlbumColumn.NAME.columnName());
      stringBuilder.append(" COLLATE NOCASE ");
      stringBuilder.append(sortOrder.value());
    }

//    System.out.println("Query: " + stringBuilder.toString());

    try (Statement statement = connection.createStatement();
         ResultSet results = statement.executeQuery(stringBuilder.toString())
    ) {
       List<String> albumsNames = new ArrayList<>();
       while (results.next()) {
         albumsNames.add(results.getString("albumName"));
       }
       return albumsNames;
    } catch (SQLException error) {
      System.out.println("Query queryAlbumsForArtist failed: " + error.getMessage());
      return null;
    }
  }

  public  List<SongArtist> queryArtistsForSong(String songName, SortOrder sortOrder) {
//  SELECT artists.name, albums.name, songs.track FROM songs
//  INNER JOIN albums ON songs.album = albums._id
//  INNER JOIN artists ON albums.artist = artists._id
//  WHERE songs.title = 'Go Your Own Way'
//  ORDER BY artists.name, albums.name COLLATE NOCASE ASC
    StringBuilder sb = new StringBuilder("SELECT " + TABLE_ARTISTS + "." + ArtistColumn.NAME.columnName() + ", " +
            TABLE_ALBUMS + "." + AlbumColumn.NAME.columnName() + ", " + TABLE_SONGS + "." + SongsColumn.TRACK.columnName() + " FROM " + TABLE_SONGS +
            " INNER JOIN " + TABLE_ALBUMS + " ON " + TABLE_SONGS + "." +
            SongsColumn.ALBUM.columnName() + " = " + TABLE_ALBUMS + "." + AlbumColumn.ID.columnName() +
            " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." +
            AlbumColumn.ARTIST.columnName() + " = " + TABLE_ARTISTS + "." + ArtistColumn.ID.columnName() +
            " WHERE " + TABLE_SONGS + "." + SongsColumn.TITLE.columnName() + " = " + "'" + songName + "'");

    if (sortOrder != SortOrder.NONE) {
      sb.append(" ORDER BY " + TABLE_ARTISTS + "." + ArtistColumn.NAME.columnName() + ", " + TABLE_ALBUMS + "." + AlbumColumn.NAME.columnName() +
              " COLLATE NOCASE " + sortOrder.value());
    }

//    System.out.println("Query: " + sb.toString());

    try (Statement statement = connection.createStatement();
         ResultSet results = statement.executeQuery(sb.toString())
    ) {
      List<SongArtist> songsArtists = new ArrayList<>();
      while (results.next()) {
        SongArtist songArtist = new SongArtist(
                results.getString(SongArtistColumn.ARTIST_NAME.columnIndex()),
                results.getString(SongArtistColumn.ALBUM_NAME.columnIndex()),
                results.getInt(SongArtistColumn.SONG_TRACK.columnIndex())
                );
        songsArtists.add(songArtist);
      }

      return songsArtists;
    } catch (SQLException error) {
      System.out.println("Query queryArtistsForSong failed: " + error.getMessage());
      return null;
    }
  }

  public  void  querySongsMetadata() {
    String sql = "SELECT * FROM " + TABLE_SONGS;

    try (Statement statement = connection.createStatement();
         ResultSet results = statement.executeQuery(sql)
    ) {
      ResultSetMetaData meta = results.getMetaData();
      int numColumns = meta.getColumnCount();
      for (int i = 1; i <= numColumns; i++) {
        System.out.format("Column %d in the songs table is names %s\n", i, meta.getColumnName(i));
      }
    } catch (SQLException error) {
      System.out.println("Query querySongsMetadata failed: " + error.getMessage());
    }
  }

  public int getCount(String tableName) {
    String sql = "SELECT COUNT(*) AS count FROM " + tableName;
    try (Statement statement = connection.createStatement();
         ResultSet results = statement.executeQuery(sql)
    ) {
      int count = results.getInt("count");
      return count;
    } catch (SQLException error) {
      System.out.println("Query getCount failed: " + error.getMessage());
      return -1;
    }
  }

  public boolean createViewForSongArtists() {
    try (Statement statement = connection.createStatement()) {
      statement.execute(CREATE_ARTIST_FOR_SONG_VIEW);
      return true;
    } catch (SQLException error) {
      System.out.println("Query createViewForSongArtists failed: " + error.getMessage());
      return false;
    }
  }
}
