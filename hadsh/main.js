"use strict";

/* Window state */
var textbox = null;
var busy = false;

/*! All users currently being managed */
var users = {};

/*! All words being managed */
var words = {};

/*! Word Adjacencies being managed */
var wordadj = {};

/*! Hostnames being managed */
var hostnames = {};

/*! Groups being managed */
var groups = {};

var auto_mark = {};
var mass_mark_legit = [];
var mass_mark_btn = null;

var newest_uid = null;
var oldest_uid = null;

/* Credit: https://stackoverflow.com/a/7124052 */
var htmlEscape = function(str) {
    return str
        .replace(/&/g, '&amp;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
};

/*!
 * Configure a XMLHttpRequest with a call-back function
 * for use with Promises.
 */
const setup_xhr = function(rq, resolve, reject) {
	rq.onreadystatechange = function() {
		if (rq.readyState == 4) {
			if (rq.status === 200) {
				resolve(rq);
			} else {
				const err = new Error('Request failed');
				err.rq = rq;
				reject(err);
			}
		}
	}
}

/*!
 * Perform a generic HTTP request
 */
const promise_http = function(uri, method, body_ct, body) {
	return new Promise(function (resolve, reject) {
		const rq = new XMLHttpRequest();
		setup_xhr(rq, resolve, reject);
		rq.open(method || 'GET', uri, true);
		if (body_ct && body) {
			rq.setRequestHeader("Content-type", body_ct);
			rq.send(body);
		} else {
			rq.send();
		}
	});
};

/*!
 * Retrieve JSON via HTTP using Promises
 */
const get_json = function(uri) {
	return promise_http(uri, 'GET').then(function (res) {
		return JSON.parse(res.responseText);
	});
};

/*!
 * Send JSON via HTTP using Promises
 */
const post_json = function(uri, data) {
	return promise_http(uri, 'POST',
		'application/json', JSON.stringify(data)
	).then(function (res) {
		if (res.responseText)
			return JSON.parse(res.responseText)
	});
};

/*!
 * A mathematical set
 */
const Set = function() {
	const self = this;
	this._elements = {};
	([].slice.apply(arguments)).forEach((e) => {
		self._elements[e] = true;
	});
};

Set.prototype.has = function(e) {
	return this._elements.hasOwnProperty(e);
};

Set.prototype.add = function() {
	const self = this;
	([].slice.apply(arguments)).forEach((e) => {
		self._elements[e] = true;
	});
};

Set.prototype.union = function() {
	const self = this;
	let res = self.clone();
	([].slice.apply(arguments)).forEach((s) => {
		res.add.apply(s.elements());
	});
	return res;
};

Set.prototype.rm = function() {
	const self = this;
	([].slice.apply(arguments)).forEach((e) => {
		if (self._elements.hasOwnProperty(e))
			delete self.elements[e];
	});
};

Set.prototype.elements = function() {
	return Object.keys(this._elements);
};

Set.prototype.clone = function() {
	const self = this;
	let clone = new Set();
	clone.add.apply(self.elements());
	return clone;
};

const ObjectSet = function() {
	const self = this;
	let args = ([].slice.apply(arguments));
	this._class = args.shift();
	this._collection = args.shift();
	this._key_fn = args.shift();
	this._obj_key = (e) => {
		if (!(e instanceof self._class))
			throw new Error('Incorrect argument type');
		return self._key_fn(e);
	};

	Set.apply(this, args.map(self._obj_key));
}

ObjectSet.prototype = Object.create(Set.prototype);

ObjectSet.prototype.has = function(e) {
	const key = this._obj_key(e);

	if (!this._collection.hasOwnProperty(key)) {
		Set.prototype.rm.call(this, key);
		return false;
	}

	return Set.call(this, key);
};

ObjectSet.prototype.add = function() {
	const self = this,
		args = [].slice.apply(arguments);
	Set.prototype.add.apply(self,
		args.map((e) => {
			const key = self._obj_key(e);
			if (!self._collection.hasOwnProperty(key))
				throw new Error('Invalid instance for set: '
					+ key);
			return key;
		}));
};

ObjectSet.prototype.rm = function() {
	const self = this,
		args = [].slice.apply(arguments);
	Set.prototype.rm.apply(self, args.map(self._obj_key));
};

ObjectSet.prototype.elements = function() {
	const self = this;
	return Object.keys(this._elements).filter((key) => {
		if (!self._collection.hasOwnProperty(key)) {
			Set.prototype.rm.call(self, key);
			return false;
		}
		return true;
	}).map((key) => {
		return self._collection[key];
	});
};

ObjectSet.prototype.clone = function() {
	const self = this;
	let clone = new ObjectSet(self._class,
		self._collection, self._key_fn);
	clone.add.apply(self.elements());
	return clone;
};

const UserSet = function() {
	let args = ([].slice.apply(arguments));
	args.unshift((user) => {
		return user.id;
	});
	args.unshift(users);
	args.unshift(User);
	ObjectSet.apply(this, args);
};
UserSet.prototype = Object.create(ObjectSet.prototype);

const WordSet = function() {
	let args = ([].slice.apply(arguments));
	args.unshift((word) => {
		return word.id;
	});
	args.unshift(words);
	args.unshift(Word);
	ObjectSet.apply(this, args);
};

WordSet.prototype = Object.create(ObjectSet.prototype);

const WordAdjSet = function() {
	let args = ([].slice.apply(arguments));
	args.unshift((wa) => {
		return wa.key;
	});
	args.unshift(wordadj);
	args.unshift(WordAdj);
	ObjectSet.apply(this, args);
};

WordAdjSet.prototype = Object.create(ObjectSet.prototype);

const HostnameSet = function() {
	let args = ([].slice.apply(arguments));
	args.unshift((h) => {
		return h.id;
	});
	args.unshift(hostnames);
	args.unshift(Hostname);
	ObjectSet.apply(this, args);
};

HostnameSet.prototype = Object.create(ObjectSet.prototype);

const GroupSet = function() {
	let args = ([].slice.apply(arguments));
	args.unshift((g) => {
		return g.name;
	});
	args.unshift(groups);
	args.unshift(Group);
	ObjectSet.apply(this, args);
};

GroupSet.prototype = Object.create(ObjectSet.prototype);

/*!
 * A scored object.
 */
const ScoredObject = function(score, count) {
	this.score = score;
	this.count = count;
	this.users = new UserSet();
};

ScoredObject.prototype.update_score = function(score, count) {
	const self = this;
	self.score = score;
	self.count = count;

	self.users.elements().forEach((u) => {
		u.update_score();
	});
};

/*!
 * A word used by one or more users.
 */
const Word = function(id, word, score, count) {
	if (words.hasOwnProperty(id))
		throw new Error('Existing word');

	words[id] = this;

	this.id = id;
	this.word = word;
	this.wordadj = new WordAdjSet();
	ScoredObject.call(this, score, count);
};
Word.prototype = Object.create(ScoredObject.prototype);

Word.from_data = function(word, data) {
	let w = words[data.id];
	if (w) {
		w.update_score(
			data.score || data.site_score,
			data.count || data.site_count);
	} else {
		w = new Word(data.id, word,
			data.score || data.site_score,
			data.count || data.site_count);
	}
	return w;
};

Word.from_id_name = function(id, word) {
	let w = words[id];
	if (!w) {
		w = new Word(id, word);
	}
	return w;
};

Word.prototype.destroy = function() {
	const self = this;

	delete words[self.id];

	self.users.elements().forEach((u) => {
		u.words.rm(self);
	});
	self.wordadj.elements().forEach((wa) => {
		wa.destroy();
	});
};

Word.prototype.update_score = function(score, count) {
	const self = this;
	self.score = score;
	self.count = count;

	self.users.elements().forEach((u) => {
		u.update_score();
	});
};

/*!
 * A pair of words that are adjacent.
 */
const WordAdj = function(proceeding, following, score, count) {
	const key = WordAdj._key_from_ids(proceeding.id, following.id);

	if (wordadj.hasOwnProperty(key))
		throw new Error('Existing word adjacency');

	wordadj[key] = this;

	this.proceeding_id = proceeding.id;
	this.following_id = following.id;
	this.key = key;
	this.score = score;
	this.count = count;
	this.users = new UserSet();

	proceeding.wordadj.add(this);
	following.wordadj.add(this);
	ScoredObject.call(this, score, count);
};
WordAdj.prototype = Object.create(ScoredObject.prototype);

WordAdj._key_from_ids = function(proceeding_id, following_id) {
	return JSON.stringify([proceeding_id, following_id]);
};

WordAdj.from_data = function(data) {
	const key = WordAdj._key_from_ids(data.proceeding_id,
					data.following_id);
	let wa = words[key];
	if (wa) {
		wa.update_score(
			data.score || data.site_score,
			data.count || data.site_count);
	} else {
		wa = new WordAdj(
			Word.from_id_name(data.proceeding_id),
			Word.from_id_name(data.following_id),
			data.score || data.site_score,
			data.count || data.site_count);
	}
	return wa;
};

WordAdj.prototype.update_score = function(score, count) {
	const self = this;
	self.score = score;
	self.count = count;

	self.users.elements().forEach((u) => {
		u.update_score();
	});
};

WordAdj.prototype.destroy = function() {
	const self = this;

	delete wordadj[self.key];

	self.users.elements().forEach((u) => {
		u.wordadj.rm(self);
	});

	if (words.hasOwnProperty(self.proceeding_id))
		words[self.proceeding_id].wordadj.rm(self);

	if (words.hasOwnProperty(self.following_id))
		words[self.following_id].wordadj.rm(self);
};

/*!
 * A hostname used by one or more users.
 */
const Hostname = function(id, hostname, score, count) {
	if (hostnames.hasOwnProperty(id))
		throw new Error('Existing hostname');

	hostnames[id] = this;

	this.id = id;
	this.hostname = hostname;
	ScoredObject.call(this, score, count);
};
Hostname.prototype = Object.create(ScoredObject.prototype);

Hostname.from_data = function(hostname, data) {
	let h = hostnames[data.id];
	if (h) {
		h.update_score(
			data.score || data.site_score,
			data.count || data.site_count);
	} else {
		h = new Hostname(data.id, hostname,
			data.score || data.site_score,
			data.count || data.site_count);
	}
	return h;
};

Hostname.from_id_name = function(id, hostname) {
	let h = hostnames[id];
	if (!h) {
		h = new Hostname(id, hostname);
	}
	return h;
};

Hostname.prototype.destroy = function() {
	const self = this;

	delete hostnames[self.id];

	self.users.elements().forEach((u) => {
		u.hostnames.rm(self);
	});
};

/*!
 * A group of users
 */
const Group = function(name) {
	if (groups.hasOwnProperty(name))
		throw new Error('Existing group');

	groups[name] = this;

	this.name = name;
	this.members = new UserSet();
};

Group.get = function(name) {
	let g = groups[name];
	if (!g) {
		g = new Group(name);
	}
	return g;
};

/*!
 * A user returned by the API
 */
const User = function(data) {
	const self = this;

	if (users.hasOwnProperty(data.id))
		throw new Error('Existing user');

	users[data.id] = this;

	self.id = data.id;
	self.groups = new GroupSet();
	self.hostnames = new HostnameSet();
	self.words = new WordSet();
	self.wordadj = new WordAdjSet();
	self.update(data);
};

User.from_data = function(data) {
	let u = users[data.id];
	if (u)
		u.update(data);
	else
		u = new User(data);
	return u;
};

User.prototype.update_score = function() {
	/* TODO */
};

User.prototype.update = function(data) {
	const self = this;

	if (data.id !== self.id)
		throw new Error('Mismatched user ID');

	self.screen_name = data.screen_name;
	self.location = data.location;
	self.about_me = data.about_me;
	self.who_am_i = data.who_am_i;
	self.tags = data.tags;
	self.links = data.links;
	self.avatar_id = data.avatar_id;
	self.created = data.created;
	self.had_created = data.had_created;
	self.last_update = data.last_update;
	self.tokens = data.tokens;
	self.next_inspection = data.next_inspection;
	self.inspections = data.inspections;
	self.pending = data.pending;
	self.url = data.url;

	let in_group = {};
	data.groups.forEach((name) => {
		const g = Group.get(name);
		in_group[name] = true;
		g.members.add(self);
		self.groups.add(g);
	});
	self.groups.elements().forEach((g) => {
		if (!in_group[g.name]) {
			self.groups.rm(g);
			g.members.rm(self);
		}
	});

	let seen_hostnames = {};
	Object.keys(data.hostnames).forEach((hostname) => {
		const hd = data.hostnames[hostname];
		const h = Hostname.from_data(hostname, hd);
		seen_hostnames[h.id] = true;
		h.users.add(self);
		self.hostnames.add(h);
	});
	self.hostnames.elements().forEach((h) => {
		if (!seen_hostnames[h.id]) {
			self.hostnames.rm(h);
			h.users.rm(self);
		}
	});

	let seen_word = {};
	Object.keys(data.words).forEach((word) => {
		const wd = data.words[word];
		const w = Word.from_data(word, wd);
		seen_word[w.id] = true;
		w.users.add(self);
		self.words.add(w);
	});
	self.words.elements().forEach((w) => {
		if (!seen_word[w.id]) {
			self.words.rm(w);
			w.users.rm(self);
		}
	});

	let seen_wordadj = {};
	data.word_adj.forEach((wordadj) => {
		const wa = WordAdj.from_data(wordadj);
		seen_wordadj[wa.key] = true;
		wa.users.add(self);
		self.wordadj.add(wa);
	});
	self.wordadj.elements().forEach((wa) => {
		if (!seen_wordadj[wa.key]) {
			self.wordadj.rm(wa);
			wa.users.rm(self);
		}
	});
};

/*!
 * Generate a style colour based on the score.
 */
const scoreColour = function (score) {
	var red = Math.round(((score > 0) ? (1.0 - score) : 1.0)*255);
	var grn = Math.round(((score < 0) ? (score + 1.0) : 1.0)*255);
	return 'rgb(' + red + ', ' + grn + ', 0)';
};

const getNextPage = function() {
	busy = true;
	var loading_msg = document.createElement('pre');
	var spinner = '-';
	var dots = '';
	textbox.appendChild(loading_msg);
	mass_mark_legit = Object.keys(auto_mark);

	if (mass_mark_btn !== null) {
		textbox.removeChild(mass_mark_btn);
	}

	if (mass_mark_legit.length > 0) {
		mass_mark_btn = document.createElement('button');
		mass_mark_btn.innerHTML = ('Mark above '
			+ mass_mark_legit.length
			+ ' auto_legit accounts as legit');
		mass_mark_btn.onclick = function() {
			mass_mark_legit.forEach(function (uid) {
				auto_mark[uid](true);
			});
			textbox.removeChild(mass_mark_btn);
			mass_mark_btn = null;
			window.scrollTo(0,0);
		};
		textbox.appendChild(mass_mark_btn);
	}

	var nextSpinner = function() {
		if (busy) {
			window.setTimeout(nextSpinner, 250);
		}

		switch (spinner) {
		case '-':	spinner = '\\';	break;
		case '\\':	spinner = '|'; break;
		case '|':	spinner = '/'; break;
		default:
				spinner = '-';
				dots += '.';
				break;
		}

		loading_msg.innerHTML = 'Loading'
			+ ((oldest_uid !== null)
				? (' users older than #' + oldest_uid)
				: (' most recent users'))
			+ dots + spinner;
	};
	nextSpinner();

	var found = 0;
	var displayed = 0;

	var uri = "/data/newcomers.json";
	if (oldest_uid !== null)
		uri += "?before_user_id=" + oldest_uid;

	get_json(uri).then(function (data) {
		// Typical action to be performed when
		// the document is ready:
		textbox.removeChild(loading_msg);

		found += data.users.length;
		data.users.forEach(function (user) {
			try {
				let u = User.from_data(user);
			} catch (err) {
				console.log('Failed to create user: '
					+ err.message
					+ '\n'
					+ err.stack);
			}

			if ((newest_uid === null)
				|| (newest_uid < user.id))
				newest_uid = user.id;

			if ((oldest_uid === null)
				|| (oldest_uid > user.id))
				oldest_uid = user.id;

			/* Hide if there's nothing to inspect */
			if (user.pending && (Object.keys(user.words).length === 0)) {
				return
			}
			displayed++;

			var userBox = document.createElement('div');
			userBox.classList.add('profile');

			var avatarBox = document.createElement('div');
			var avatar = document.createElement('img');
			avatar.src = '/avatar/' + user.avatar_id
				+ '?width=100&height=100';
			avatarBox.classList.add('avatar_box');
			avatarBox.appendChild(avatar);
			userBox.appendChild(avatarBox);

			var profile_link = document.createElement('a');
			profile_link.href = user.url;
			var profile_name = document.createElement('tt');
			profile_name.innerHTML = user.screen_name;
			profile_link.appendChild(profile_name);
			userBox.appendChild(profile_link);

			var profile_uid = document.createTextNode(' ' + user.id);
			userBox.appendChild(profile_uid);

			if (user.pending) {
				userBox.appendChild(
					document.createTextNode(' Re-inspection pending ('
						+ user.next_inspection
						+ '; '
						+ user.inspections
						+ ' inspections)')
				);
			}

			var profile_score = document.createElement('div');
			userBox.appendChild(profile_score);

			var profile_score_gauge = document.createElement('div');
			profile_score_gauge.classList.add('score_gauge');
			profile_score_gauge.classList.add('score_gauge_base');
			var profile_score_gauge_left = document.createElement('div');
			profile_score_gauge_left.classList.add('score_gauge');
			profile_score_gauge_left.classList.add('score_gauge_indication');
			profile_score_gauge.appendChild(profile_score_gauge_left);
			var profile_score_gauge_bar = document.createElement('div');
			profile_score_gauge_bar.classList.add('score_gauge');
			profile_score_gauge_bar.classList.add('score_gauge_indication');
			profile_score_gauge.appendChild(profile_score_gauge_bar);
			var profile_score_gauge_right = document.createElement('div');
			profile_score_gauge_right.classList.add('score_gauge');
			profile_score_gauge_right.classList.add('score_gauge_indication');
			profile_score_gauge.appendChild(profile_score_gauge_right);
			userBox.appendChild(profile_score_gauge);

			var profile_created = document.createElement('div');
			profile_created.innerHTML = user.had_created || user.created;
			userBox.appendChild(profile_created);

			var profile_groups = document.createElement('div');
			var group_set = {};
			user.groups.forEach(function (group) {
				var group_label = document.createElement('tt');
				group_label.innerHTML = group;
				profile_groups.appendChild(group_label);
				group_set[group] = true;
			});

			var rm_auto = function() {
				if (auto_mark[user.id] !== undefined) {
					delete auto_mark[user.id];
				}
			};

			if (!group_set.legit) {
				var classify_legit = document.createElement('button');
				classify_legit.innerHTML = 'Legit';
				var do_classify = function(mass_update) {
					rm_auto();
					post_json('/classify/' + user.id,
						"legit"
					).then(function() {
						setTimeout(function () {
							textbox.removeChild(userBox);
						}, ((mass_update === true) ? 500 : 10000));
					});
					profile_groups.removeChild(classify_legit);
				};
				if (group_set.auto_legit && (!user.pending)) {
					auto_mark[user.id] = do_classify;
				}

				classify_legit.onclick = do_classify;
				profile_groups.appendChild(classify_legit);
			}
			if (!group_set.suspect) {
				var classify_suspect = document.createElement('button');
				classify_suspect.innerHTML = 'Suspect';
				classify_suspect.onclick = function() {
					rm_auto();
					post_json('/classify/' + user.id,
						"suspect"
					).then(function() {
						setTimeout(function () {
							textbox.removeChild(userBox);
						}, 10000);
					});
					profile_groups.removeChild(classify_suspect);
				};
				profile_groups.appendChild(classify_suspect);
			}

			var defer_classify = document.createElement('button');
			defer_classify.innerHTML = 'Defer';
			defer_classify.onclick = function() {
				rm_auto();
				textbox.removeChild(userBox);
			};
			profile_groups.appendChild(defer_classify);

			userBox.appendChild(profile_groups);

			var profile_tags = document.createElement('div');
			user.tags.forEach(function (tag) {
				var tag_label = document.createElement('tt');
				tag_label.innerHTML = tag;
				profile_tags.appendChild(tag_label);
			});
			userBox.appendChild(profile_tags);

			if (user.location) {
				var profile_location = document.createElement('div');
				profile_location.innerHTML = user.location;
				userBox.appendChild(profile_location);
			}

			if (user.about_me) {
				var profile_about_me = document.createElement('div');
				profile_about_me.innerHTML = user.about_me;
				userBox.appendChild(profile_about_me);
			}

			if (user.who_am_i) {
				var profile_who_am_i = document.createElement('div');
				profile_who_am_i.innerHTML = user.who_am_i;
				userBox.appendChild(profile_who_am_i);
			}

			if (user.projects) {
				var profile_projects = document.createElement('div');
				profile_projects.innerHTML = user.projects + ' project(s)';
				userBox.appendChild(profile_projects);
			}

			if (user.what_i_would_like_to_do) {
				var profile_what_i_would_like_to_do = document.createElement('div');
				profile_what_i_would_like_to_do.innerHTML = user.what_i_would_like_to_do;
				userBox.appendChild(profile_what_i_would_like_to_do);
			}

			var links = document.createElement('ul');
			user.links.forEach(function (link) {
				var link_tag = document.createElement('a');
				link_tag.href = link.url;
				link_tag.appendChild(document.createTextNode(link.title + ' '));
				var link_tt = document.createElement('tt');
				link_tt.appendChild(document.createTextNode(
					'<' + htmlEscape(link.url) + '>'));
				link_tag.appendChild(link_tt);

				var link_item = document.createElement('li');
				link_item.appendChild(link_tag);
				links.appendChild(link_item);
			});
			userBox.appendChild(links);

			if (user.tokens && Object.keys(user.tokens).length) {
				var profile_tokens = document.createElement('ul');
				Object.keys(user.tokens).forEach(function (token) {
					var token_li = document.createElement('li');
					var token_tt = document.createElement('tt');
					token_tt.innerHTML = htmlEscape(token);
					token_li.appendChild(token_tt);
					token_li.appendChild(document.createTextNode(' ' + user.tokens[token] + ' instances'));
					profile_tokens.appendChild(token_li);
				});
				userBox.appendChild(profile_tokens);
			}

			/* Compute the user's score */
			var user_score = [];
			var first;
			if (user.hostnames && Object.keys(user.hostnames).length) {
				var profile_hostnames = document.createElement('div');
				first = false;
				var hostnames = Object.keys(user.hostnames).map(function (hostname) {
					var stat = user.hostnames[hostname];
					var score = 0.0;
					if (stat.site_count > 0) {
						score = Math.round((stat.site_score * 100)
							/ stat.site_count) / 100;
						user_score.push(score);
					}
					stat.hostname = hostname;
					stat.norm_score = score;
					return stat;
				});
				hostnames.sort(function (a, b) {
					if (a.norm_score < b.norm_score)
						return -1;
					else if (a.norm_score > b.norm_score)
						return 1;
					return 0;
				});
				hostnames.forEach(function (stat) {
					var hostname = stat.hostname;
					var hostname_span = document.createElement('span');
					var hostname_tt = document.createElement('tt');
					var score = stat.norm_score;

					hostname_tt.innerHTML = htmlEscape(hostname);
					hostname_span.appendChild(hostname_tt);
					hostname_span.classList.add('word');
					hostname_span.title = stat.user_count
						+ ' occurances; score: '
						+ score;
					hostname_span.style.backgroundColor = scoreColour(score);
					if (first) {
						profile_hostnames.appendChild(
							document.createTextNode(' ')
						);
					} else {
						first = true;
					}
					profile_hostnames.appendChild(hostname_span);
				});
				userBox.appendChild(profile_hostnames);
			}

			if (user.words && Object.keys(user.words).length) {
				var profile_words = document.createElement('div');
				first = false;
				var words = Object.keys(user.words).map(function (word) {
					var stat = user.words[word];
					var score = 0.0;
					if (stat.site_count > 0) {
						score = Math.round((stat.site_score * 100)
							/ stat.site_count) / 100;
						user_score.push(score);
					}
					stat.word = word;
					stat.norm_score = score;
					return stat;
				});
				words.sort(function (a, b) {
					if (a.norm_score < b.norm_score)
						return -1;
					else if (a.norm_score > b.norm_score)
						return 1;
					return 0;
				});
				words.forEach(function (stat) {
					var word = stat.word;
					var word_span = document.createElement('span');
					var word_tt = document.createElement('tt');
					var score = stat.norm_score;

					word_tt.innerHTML = htmlEscape(word);
					word_span.appendChild(word_tt);
					word_span.classList.add('word');
					word_span.title = stat.user_count
						+ ' occurances; score: '
						+ score;
					word_span.style.backgroundColor = scoreColour(score);
					if (first) {
						profile_words.appendChild(
							document.createTextNode(' ')
						);
					} else {
						first = true;
					}
					profile_words.appendChild(word_span);
				});
				userBox.appendChild(profile_words);
			}

			if (user.word_adj && user.word_adj.length) {
				var profile_word_adj = document.createElement('div');
				first = false;
				var word_adjs = user.word_adj.map(function (word_adj) {
					var score = 0.0;
					if (word_adj.site_count > 0) {
						score = Math.round((word_adj.site_score * 100)
							/ word_adj.site_count) / 100;
						user_score.push(score);
					}
					word_adj.norm_score = score;
					return word_adj;
				});
				word_adjs.sort(function (a, b) {
					if (a.norm_score < b.norm_score)
						return -1;
					else if (a.norm_score > b.norm_score)
						return 1;
					return 0;
				});
				word_adjs.forEach(function (word_adj) {
					var adj_span = document.createElement('span');
					var adj_tt = document.createElement('tt');
					var score = word_adj.norm_score;

					adj_tt.innerHTML = htmlEscape(word_adj.proceeding)
						+ ' &rarr; '
						+ htmlEscape(word_adj.following);
					adj_span.appendChild(adj_tt);
					adj_span.classList.add('word');
					adj_span.title = word_adj.user_count
						+ ' occurances; score: '
						+ score;
					if (first) {
						profile_word_adj.appendChild(
							document.createTextNode(' ')
						);
					} else {
						first = true;
					}

					profile_word_adj.appendChild(adj_span);

					/* Derive span colour */
					adj_span.style.backgroundColor = scoreColour(score);
				});
				userBox.appendChild(profile_word_adj);
			}

			/* Compute user score */
			if (user_score.length) {
				user_score = Math.round(100*user_score.sort(function (a, b) {
					if (a < b)
						return -1;
					else if (a > b)
						return 1;
					return 0;
				}).slice(0, 5).reduce(function (a, b) {
					return a + b;
				})) / 100;
			} else {
				user_score = 0.0;
			}
			profile_score.innerHTML = 'Score: ' + user_score;

			if (user_score < 0.0) {
				profile_score_gauge_left.style.width = (16 * (10.0 + (2*user_score))) + 'px';
				profile_score_gauge_bar.style.width = (16 * (-(2*user_score))) + 'px';
				profile_score_gauge_right.style.width = '160px';
			} else if (user_score > 0.0) {
				profile_score_gauge_left.style.width = '160px';
				profile_score_gauge_bar.style.width = (16 * (2*user_score)) + 'px';
				profile_score_gauge_right.style.width = (16 * (10.0 - (2*user_score))) + 'px';
			} else {
				profile_score_gauge_left.style.width = '155px';
				profile_score_gauge_bar.style.width = '10px';
				profile_score_gauge_right.style.width = '155px';
			}
			profile_score_gauge_bar.style.backgroundColor = scoreColour(user_score);

			textbox.appendChild(userBox);
		});

		if (found && !displayed) {
			setTimeout(getNextPage, 10);
		}
		busy = false;
	}).catch(function (err) {
		busy = false;
		console.log('Error ' + err.rq.status + ' retrieving data');
	});
};

var main = function() {
	window.onscroll = function(ev) {
		if ((window.innerHeight + window.scrollY)
			>= document.body.offsetHeight) {
			if (!busy)
				getNextPage();
		}
	};

	textbox = document.getElementById('recent');
	getNextPage();
};
